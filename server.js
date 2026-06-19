require('dotenv').config();

const express = require('express');
const mysql = require('mysql2/promise');
const cors = require('cors');
const { CognitoJwtVerifier } = require('aws-jwt-verify');

const app = express();

app.use(cors());
app.use(express.json());

// ================================
// DATABASE POOL
// ================================
const dbPool = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    waitForConnections: true,
    connectionLimit: 10
});

// ================================
// COGNITO JWT VERIFIER
// ================================
const verifier = CognitoJwtVerifier.create({
    userPoolId: process.env.COGNITO_USER_POOL_ID,
    tokenUse: "access",
    clientId: process.env.COGNITO_APP_CLIENT_ID
});

// ================================
// AUTH MIDDLEWARE
// ================================
async function authMiddleware(req, res, next) {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Bearer ')) {
        return res.status(401).json({
            error: "Không tìm thấy Token. Vui lòng đăng nhập!"
        });
    }

    const token = authHeader.split(' ')[1];

    try {
        const payload = await verifier.verify(token);

        req.user = {
            sub: payload.sub,
            username: payload.username || payload["cognito:username"] || payload.sub,
            groups: payload["cognito:groups"] || [],
            accessToken: token,
            payload
        };

        next();
    } catch (error) {
        console.error("Lỗi verify token:", error);

        return res.status(401).json({
            error: "Token không hợp lệ hoặc đã hết hạn!"
        });
    }
}

// ================================
// HELPER FUNCTIONS
// ================================
function maskBankAccountNumber(accountNumber) {
    if (!accountNumber) return "";

    const clean = String(accountNumber).trim();

    if (clean.length <= 4) {
        return clean;
    }

    return "****" + clean.slice(-4);
}

function buildDisplayName(methodType, data) {
    if (methodType === "COD") {
        return "Thanh toán khi nhận hàng";
    }

    if (methodType === "MOMO") {
        return `MoMo - ${data.momoPhoneNumber}`;
    }

    if (methodType === "BANK") {
        return `${data.bankName} - ${maskBankAccountNumber(data.bankAccountNumber)}`;
    }

    return "Phương thức thanh toán";
}

async function ensureCodPaymentMethod(connection, userId) {
    const [codRows] = await connection.execute(
        `
        SELECT payment_method_id
        FROM user_payment_methods
        WHERE user_id = ?
          AND method_type = 'COD'
        LIMIT 1
        `,
        [userId]
    );

    if (codRows.length > 0) {
        return codRows[0].payment_method_id;
    }

    const [defaultRows] = await connection.execute(
        `
        SELECT payment_method_id
        FROM user_payment_methods
        WHERE user_id = ?
          AND is_default = TRUE
        LIMIT 1
        `,
        [userId]
    );

    const shouldBeDefault = defaultRows.length === 0;

    const [result] = await connection.execute(
        `
        INSERT INTO user_payment_methods (
            user_id,
            method_type,
            display_name,
            is_default,
            is_system_default
        )
        VALUES (?, 'COD', 'Thanh toán khi nhận hàng', ?, TRUE)
        `,
        [userId, shouldBeDefault]
    );

    return result.insertId;
}

async function getCodPaymentMethodId(connection, userId) {
    const [rows] = await connection.execute(
        `
        SELECT payment_method_id
        FROM user_payment_methods
        WHERE user_id = ?
          AND method_type = 'COD'
        LIMIT 1
        `,
        [userId]
    );

    if (rows.length === 0) {
        return await ensureCodPaymentMethod(connection, userId);
    }

    return rows[0].payment_method_id;
}

// ==========================================
// ROUTE 1: LẤY DANH SÁCH PHƯƠNG THỨC THANH TOÁN
// ==========================================
app.get('/api/payments/me/payment-methods', authMiddleware, async (req, res) => {
    const userId = req.user.sub;
    let connection;

    try {
        connection = await dbPool.getConnection();

        await ensureCodPaymentMethod(connection, userId);

        const [rows] = await connection.execute(
            `
            SELECT
                payment_method_id,
                user_id,
                method_type,
                display_name,
                momo_phone_number,
                bank_name,
                bank_account_number,
                is_default,
                is_system_default,
                created_at,
                updated_at
            FROM user_payment_methods
            WHERE user_id = ?
            ORDER BY is_default DESC, is_system_default DESC, created_at DESC
            `,
            [userId]
        );

        return res.json({
            message: "Lấy danh sách phương thức thanh toán thành công!",
            paymentMethods: rows
        });

    } catch (error) {
        console.error("Lỗi lấy payment methods:", error);

        return res.status(500).json({
            error: "Không thể lấy danh sách phương thức thanh toán!"
        });

    } finally {
        if (connection) connection.release();
    }
});

// ==========================================
// ROUTE 2: THÊM PHƯƠNG THỨC THANH TOÁN
// ==========================================
app.post('/api/payments/me/payment-methods', authMiddleware, async (req, res) => {
    const userId = req.user.sub;

    const {
        methodType,
        momoPhoneNumber,
        bankName,
        bankAccountNumber
    } = req.body || {};

    const normalizedMethodType = methodType ? methodType.trim().toUpperCase() : "";

    if (!["MOMO", "BANK"].includes(normalizedMethodType)) {
        return res.status(400).json({
            error: "Loại phương thức thanh toán không hợp lệ. Chỉ chấp nhận MOMO hoặc BANK."
        });
    }

    if (normalizedMethodType === "MOMO") {
        if (!momoPhoneNumber || !String(momoPhoneNumber).trim()) {
            return res.status(400).json({
                error: "Vui lòng nhập số điện thoại MoMo."
            });
        }
    }

    if (normalizedMethodType === "BANK") {
        if (!bankName || !String(bankName).trim()) {
            return res.status(400).json({
                error: "Vui lòng chọn ngân hàng."
            });
        }

        if (!bankAccountNumber || !String(bankAccountNumber).trim()) {
            return res.status(400).json({
                error: "Vui lòng nhập số tài khoản ngân hàng."
            });
        }
    }

    let connection;

    try {
        connection = await dbPool.getConnection();

        await ensureCodPaymentMethod(connection, userId);

        const displayName = buildDisplayName(normalizedMethodType, {
            momoPhoneNumber: momoPhoneNumber ? String(momoPhoneNumber).trim() : null,
            bankName: bankName ? String(bankName).trim() : null,
            bankAccountNumber: bankAccountNumber ? String(bankAccountNumber).trim() : null
        });

        const [result] = await connection.execute(
            `
            INSERT INTO user_payment_methods (
                user_id,
                method_type,
                display_name,
                momo_phone_number,
                bank_name,
                bank_account_number,
                is_default,
                is_system_default
            )
            VALUES (?, ?, ?, ?, ?, ?, FALSE, FALSE)
            `,
            [
                userId,
                normalizedMethodType,
                displayName,
                normalizedMethodType === "MOMO" ? String(momoPhoneNumber).trim() : null,
                normalizedMethodType === "BANK" ? String(bankName).trim() : null,
                normalizedMethodType === "BANK" ? String(bankAccountNumber).trim() : null
            ]
        );

        const [createdRows] = await connection.execute(
            `
            SELECT
                payment_method_id,
                user_id,
                method_type,
                display_name,
                momo_phone_number,
                bank_name,
                bank_account_number,
                is_default,
                is_system_default,
                created_at,
                updated_at
            FROM user_payment_methods
            WHERE payment_method_id = ?
              AND user_id = ?
            `,
            [result.insertId, userId]
        );

        return res.status(201).json({
            message: "Liên kết phương thức thanh toán thành công!",
            paymentMethod: createdRows[0]
        });

    } catch (error) {
        console.error("Lỗi thêm payment method:", error);

        return res.status(500).json({
            error: "Không thể thêm phương thức thanh toán!"
        });

    } finally {
        if (connection) connection.release();
    }
});

// ==========================================
// ROUTE 3: XÓA PHƯƠNG THỨC THANH TOÁN
// ==========================================
app.delete('/api/payments/me/payment-methods/:paymentMethodId', authMiddleware, async (req, res) => {
    const userId = req.user.sub;
    const { paymentMethodId } = req.params;

    let connection;

    try {
        connection = await dbPool.getConnection();
        await connection.beginTransaction();

        await ensureCodPaymentMethod(connection, userId);

        const [rows] = await connection.execute(
            `
            SELECT
                payment_method_id,
                method_type,
                is_default,
                is_system_default
            FROM user_payment_methods
            WHERE payment_method_id = ?
              AND user_id = ?
            LIMIT 1
            `,
            [paymentMethodId, userId]
        );

        if (rows.length === 0) {
            await connection.rollback();

            return res.status(404).json({
                error: "Không tìm thấy phương thức thanh toán cần xóa!"
            });
        }

        const paymentMethod = rows[0];

        if (paymentMethod.method_type === "COD" || paymentMethod.is_system_default) {
            await connection.rollback();

            return res.status(400).json({
                error: "Không thể xóa phương thức COD mặc định của hệ thống!"
            });
        }

        const wasDefault = Boolean(paymentMethod.is_default);

        await connection.execute(
            `
            DELETE FROM user_payment_methods
            WHERE payment_method_id = ?
              AND user_id = ?
            `,
            [paymentMethodId, userId]
        );

        if (wasDefault) {
            const codPaymentMethodId = await getCodPaymentMethodId(connection, userId);

            await connection.execute(
                `
                UPDATE user_payment_methods
                SET is_default = FALSE
                WHERE user_id = ?
                `,
                [userId]
            );

            await connection.execute(
                `
                UPDATE user_payment_methods
                SET is_default = TRUE
                WHERE user_id = ?
                  AND payment_method_id = ?
                `,
                [userId, codPaymentMethodId]
            );
        }

        await connection.commit();

        return res.json({
            message: wasDefault
                ? "Đã xóa phương thức thanh toán. Mặc định đã chuyển về COD."
                : "Đã xóa phương thức thanh toán thành công!"
        });

    } catch (error) {
        if (connection) {
            await connection.rollback();
        }

        console.error("Lỗi xóa payment method:", error);

        return res.status(500).json({
            error: "Không thể xóa phương thức thanh toán!"
        });

    } finally {
        if (connection) connection.release();
    }
});

// ==========================================
// ROUTE 4: CHỌN PHƯƠNG THỨC THANH TOÁN MẶC ĐỊNH
// ==========================================
app.put('/api/payments/me/payment-methods/:paymentMethodId/default', authMiddleware, async (req, res) => {
    const userId = req.user.sub;
    const { paymentMethodId } = req.params;

    let connection;

    try {
        connection = await dbPool.getConnection();
        await connection.beginTransaction();

        await ensureCodPaymentMethod(connection, userId);

        const [rows] = await connection.execute(
            `
            SELECT payment_method_id
            FROM user_payment_methods
            WHERE payment_method_id = ?
              AND user_id = ?
            LIMIT 1
            `,
            [paymentMethodId, userId]
        );

        if (rows.length === 0) {
            await connection.rollback();

            return res.status(404).json({
                error: "Không tìm thấy phương thức thanh toán!"
            });
        }

        await connection.execute(
            `
            UPDATE user_payment_methods
            SET is_default = FALSE
            WHERE user_id = ?
            `,
            [userId]
        );

        await connection.execute(
            `
            UPDATE user_payment_methods
            SET is_default = TRUE
            WHERE user_id = ?
              AND payment_method_id = ?
            `,
            [userId, paymentMethodId]
        );

        await connection.commit();

        const [updatedRows] = await connection.execute(
            `
            SELECT
                payment_method_id,
                user_id,
                method_type,
                display_name,
                momo_phone_number,
                bank_name,
                bank_account_number,
                is_default,
                is_system_default,
                created_at,
                updated_at
            FROM user_payment_methods
            WHERE payment_method_id = ?
              AND user_id = ?
            `,
            [paymentMethodId, userId]
        );

        return res.json({
            message: "Đã chọn phương thức thanh toán mặc định!",
            paymentMethod: updatedRows[0]
        });

    } catch (error) {
        if (connection) {
            await connection.rollback();
        }

        console.error("Lỗi chọn payment method mặc định:", error);

        return res.status(500).json({
            error: "Không thể chọn phương thức thanh toán mặc định!"
        });

    } finally {
        if (connection) connection.release();
    }
});

// ================================
// START SERVER
// ================================
const PORT = process.env.PORT || 3003;

app.listen(PORT, () => {
    console.log(`Payment Service running on port ${PORT}`);
});