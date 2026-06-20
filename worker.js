require('dotenv').config();

const AWS = require('aws-sdk');
const mysql = require('mysql2/promise');
const crypto = require('crypto');

AWS.config.update({
    region: process.env.AWS_REGION || 'ap-southeast-1'
});

const sqs = new AWS.SQS();
const sns = new AWS.SNS();

const dbPool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10
});

const QUEUE_URL = process.env.PAYMENT_REQUESTED_QUEUE_URL;
const PAYMENT_RESULT_TOPIC_ARN = process.env.PAYMENT_RESULT_TOPIC_ARN;

const PAYMENT_SUCCESS_LIMIT = 1000000;

if (!QUEUE_URL) {
    console.error('Thiếu PAYMENT_REQUESTED_QUEUE_URL trong .env');
    process.exit(1);
}

if (!PAYMENT_RESULT_TOPIC_ARN) {
    console.error('Thiếu PAYMENT_RESULT_TOPIC_ARN trong .env');
    process.exit(1);
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function parseSqsMessageBody(body) {
    const parsed = JSON.parse(body);

    if (parsed.eventType) {
        return parsed;
    }

    if (parsed.Message) {
        return JSON.parse(parsed.Message);
    }

    return parsed;
}

function buildTransactionId(orderId) {
    const random = crypto.randomBytes(6).toString('hex').toUpperCase();
    return `PAY-${orderId}-${Date.now()}-${random}`;
}

function validatePaymentRequestedEvent(event) {
    if (!event) {
        throw new Error('Message rỗng!');
    }

    if (event.eventType !== 'PaymentRequested') {
        throw new Error(`eventType không hợp lệ: ${event.eventType}`);
    }

    if (!event.orderId) {
        throw new Error('Thiếu orderId trong PaymentRequested!');
    }

    if (!event.userId) {
        throw new Error('Thiếu userId trong PaymentRequested!');
    }

    const amount = Number(event.amount);

    if (Number.isNaN(amount) || amount <= 0) {
        throw new Error('amount không hợp lệ trong PaymentRequested!');
    }

    const methodType = String(event.paymentMethod?.methodType || '').toUpperCase();

    if (!['MOMO', 'BANK'].includes(methodType)) {
        throw new Error(`Payment method không hỗ trợ trong worker: ${methodType}`);
    }

    return {
        orderId: Number(event.orderId),
        userId: event.userId,
        amount,
        currency: event.currency || 'VND',
        paymentMethodId: event.paymentMethod?.paymentMethodId || null,
        paymentMethodType: methodType,
        paymentMethodDisplayName: event.paymentMethod?.displayName || null
    };
}

async function getExistingTransaction(orderId) {
    const [rows] = await dbPool.execute(
        `
        SELECT
            payment_transaction_id,
            order_id,
            user_id,
            amount,
            currency,
            payment_method_id,
            payment_method_type,
            payment_status,
            failure_reason
        FROM payment_transactions
        WHERE order_id = ?
        LIMIT 1
        `,
        [orderId]
    );

    return rows.length > 0 ? rows[0] : null;
}

async function savePaymentTransaction({
    paymentTransactionId,
    orderId,
    userId,
    amount,
    currency,
    paymentMethodId,
    paymentMethodType,
    paymentStatus,
    failureReason,
    rawEvent
}) {
    await dbPool.execute(
        `
        INSERT INTO payment_transactions (
            payment_transaction_id,
            order_id,
            user_id,
            amount,
            currency,
            payment_method_id,
            payment_method_type,
            payment_status,
            failure_reason,
            raw_event
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
            paymentTransactionId,
            orderId,
            userId,
            amount,
            currency,
            paymentMethodId,
            paymentMethodType,
            paymentStatus,
            failureReason,
            JSON.stringify(rawEvent)
        ]
    );
}

async function publishPaymentResult(paymentResultEvent) {
    const result = await sns.publish({
        TopicArn: PAYMENT_RESULT_TOPIC_ARN,
        Message: JSON.stringify(paymentResultEvent),
        MessageAttributes: {
            eventType: {
                DataType: 'String',
                StringValue: 'PaymentResult'
            },
            paymentStatus: {
                DataType: 'String',
                StringValue: paymentResultEvent.paymentStatus
            }
        }
    }).promise();

    return result.MessageId;
}

async function deleteMessage(receiptHandle) {
    await sqs.deleteMessage({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: receiptHandle
    }).promise();
}

async function processPaymentRequested(event) {
    const paymentRequest = validatePaymentRequestedEvent(event);

    const existingTransaction = await getExistingTransaction(paymentRequest.orderId);

    if (existingTransaction) {
        console.log(
            `[SKIP] Order ${paymentRequest.orderId} đã có transaction ${existingTransaction.payment_transaction_id} với status ${existingTransaction.payment_status}`
        );

        const paymentResultEvent = {
            eventType: 'PaymentResult',
            eventVersion: '1.0',
            orderId: Number(existingTransaction.order_id),
            userId: existingTransaction.user_id,
            paymentStatus: existingTransaction.payment_status,
            paymentTransactionId: existingTransaction.payment_transaction_id,
            paymentError: existingTransaction.failure_reason,
            amount: Number(existingTransaction.amount),
            currency: existingTransaction.currency || 'VND',
            paymentMethod: {
                paymentMethodId: existingTransaction.payment_method_id,
                methodType: existingTransaction.payment_method_type
            },
            processedAt: new Date().toISOString(),
            duplicated: true
        };

        const messageId = await publishPaymentResult(paymentResultEvent);

        return {
            skipped: true,
            paymentStatus: existingTransaction.payment_status,
            paymentTransactionId: existingTransaction.payment_transaction_id,
            paymentResultMessageId: messageId
        };
    }

    const isSuccess = paymentRequest.amount <= PAYMENT_SUCCESS_LIMIT;

    const paymentStatus = isSuccess ? 'PAID' : 'FAILED';

    const failureReason = isSuccess
        ? null
        : `Fake payment failed: amount ${paymentRequest.amount} > ${PAYMENT_SUCCESS_LIMIT}`;

    const paymentTransactionId = buildTransactionId(paymentRequest.orderId);

    console.log(
        `[PROCESS] orderId=${paymentRequest.orderId}, amount=${paymentRequest.amount}, method=${paymentRequest.paymentMethodType}, result=${paymentStatus}`
    );

    // Giả lập thời gian xử lý thanh toán
    await sleep(3000);

    await savePaymentTransaction({
        paymentTransactionId,
        orderId: paymentRequest.orderId,
        userId: paymentRequest.userId,
        amount: paymentRequest.amount,
        currency: paymentRequest.currency,
        paymentMethodId: paymentRequest.paymentMethodId,
        paymentMethodType: paymentRequest.paymentMethodType,
        paymentStatus,
        failureReason,
        rawEvent: event
    });

    const paymentResultEvent = {
        eventType: 'PaymentResult',
        eventVersion: '1.0',
        orderId: paymentRequest.orderId,
        userId: paymentRequest.userId,
        paymentStatus,
        paymentTransactionId,
        paymentError: failureReason,
        amount: paymentRequest.amount,
        currency: paymentRequest.currency,
        paymentMethod: {
            paymentMethodId: paymentRequest.paymentMethodId,
            methodType: paymentRequest.paymentMethodType,
            displayName: paymentRequest.paymentMethodDisplayName
        },
        processedAt: new Date().toISOString()
    };

    const paymentResultMessageId = await publishPaymentResult(paymentResultEvent);

    return {
        skipped: false,
        paymentStatus,
        paymentTransactionId,
        paymentError: failureReason,
        paymentResultMessageId
    };
}

async function processMessage(message) {
    const receiptHandle = message.ReceiptHandle;

    try {
        const event = parseSqsMessageBody(message.Body);

        console.log('[MESSAGE] Received event:', JSON.stringify(event));

        const result = await processPaymentRequested(event);

        console.log('[DONE] Payment result published:', result);

        await deleteMessage(receiptHandle);

        console.log('[DELETE] SQS payment-requested message deleted.');

    } catch (error) {
        console.error('[ERROR] Không thể xử lý message:', error.message);

        // Không delete message nếu lỗi kỹ thuật.
        // SQS sẽ retry sau VisibilityTimeout.
    }
}

async function pollMessages() {
    console.log('Payment worker started.');
    console.log(`Listening queue: ${QUEUE_URL}`);
    console.log(`Publishing result topic: ${PAYMENT_RESULT_TOPIC_ARN}`);
    console.log(`Success rule: amount <= ${PAYMENT_SUCCESS_LIMIT}`);
    console.log(`Fail rule: amount > ${PAYMENT_SUCCESS_LIMIT}`);

    while (true) {
        try {
            const result = await sqs.receiveMessage({
                QueueUrl: QUEUE_URL,
                MaxNumberOfMessages: 5,
                WaitTimeSeconds: 20,
                VisibilityTimeout: 60
            }).promise();

            const messages = result.Messages || [];

            if (messages.length === 0) {
                continue;
            }

            console.log(`[POLL] Received ${messages.length} message(s).`);

            for (const message of messages) {
                await processMessage(message);
            }

        } catch (error) {
            console.error('[POLL ERROR]', error.message);
            await sleep(3000);
        }
    }
}

process.on('SIGINT', async () => {
    console.log('Payment worker received SIGINT. Exiting...');
    await dbPool.end();
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('Payment worker received SIGTERM. Exiting...');
    await dbPool.end();
    process.exit(0);
});

pollMessages();