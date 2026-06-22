CREATE DATABASE IF NOT EXISTS ecommerce_payment_db;
USE ecommerce_payment_db;

CREATE TABLE IF NOT EXISTS user_payment_methods (
    payment_method_id INT AUTO_INCREMENT PRIMARY KEY,

    user_id VARCHAR(128) NOT NULL,

    method_type VARCHAR(20) NOT NULL,
    display_name VARCHAR(255) NOT NULL,

    momo_phone_number VARCHAR(20),

    bank_name VARCHAR(100),
    bank_account_number VARCHAR(50),

    is_default BOOLEAN DEFAULT FALSE,
    is_system_default BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_user_id (user_id),
    INDEX idx_user_default (user_id, is_default)
);
CREATE TABLE IF NOT EXISTS payment_transactions (
    payment_transaction_id VARCHAR(100) PRIMARY KEY,
    order_id BIGINT NOT NULL UNIQUE,
    user_id VARCHAR(128),
    amount DECIMAL(12,2) NOT NULL,
    currency VARCHAR(10) DEFAULT 'VND',
    payment_method_id BIGINT,
    payment_method_type VARCHAR(20),
    payment_status VARCHAR(20) NOT NULL,
    failure_reason TEXT,
    raw_event JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);