-- MySQL 8.0 Schema for AWS re:Post Crawler
-- 創建資料庫（如果不存在）
CREATE DATABASE IF NOT EXISTS repost_crawler
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

-- 使用該資料庫
USE repost_crawler;

-- 設置嚴格模式和時區
SET sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';
SET time_zone = '+00:00';

-- 刪除已存在的表（如果需要重新創建）
DROP TABLE IF EXISTS question_tags;
DROP TABLE IF EXISTS crawler_executions;
DROP TABLE IF EXISTS tags;
DROP TABLE IF EXISTS questions;

-- 創建 questions 表
CREATE TABLE questions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    question_id VARCHAR(100) NOT NULL UNIQUE COMMENT 'AWS re:Post 問題的唯一識別碼',
    title VARCHAR(500) NOT NULL COMMENT '問題標題',
    description TEXT COMMENT '問題描述',
    language ENUM('en', 'zh-Hant') NOT NULL COMMENT '問題語言',
    url VARCHAR(500) NOT NULL COMMENT '問題URL',
    view_count INT NOT NULL DEFAULT 0 COMMENT '瀏覽次數',
    vote_count INT NOT NULL DEFAULT 0 COMMENT '投票數',
    answers_count INT NOT NULL DEFAULT 0 COMMENT '回答數',
    has_accepted_answer BOOLEAN NOT NULL DEFAULT FALSE COMMENT '是否有已接受的答案',
    posted_at TIMESTAMP NULL COMMENT '發布時間',
    crawled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '爬取時間',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最後更新時間',
    INDEX idx_language (language),
    INDEX idx_posted_at (posted_at),
    INDEX idx_crawled_at (crawled_at),
    INDEX idx_url (url(255))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='AWS re:Post 問題資料表';

-- 創建 tags 表
CREATE TABLE tags (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE COMMENT '標籤名稱',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='標籤資料表';

-- 創建 question_tags 關聯表
CREATE TABLE question_tags (
    question_id BIGINT NOT NULL COMMENT '問題ID',
    tag_id BIGINT NOT NULL COMMENT '標籤ID',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '創建時間',
    PRIMARY KEY (question_id, tag_id),
    FOREIGN KEY (question_id) REFERENCES questions(id) ON DELETE CASCADE,
    FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='問題-標籤關聯表';

-- 創建 crawler_executions 表
CREATE TABLE crawler_executions (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    start_time TIMESTAMP NOT NULL COMMENT '開始時間',
    end_time TIMESTAMP NOT NULL COMMENT '結束時間',
    questions_processed INT NOT NULL DEFAULT 0 COMMENT '處理的問題數量',
    english_questions INT NOT NULL DEFAULT 0 COMMENT '英文問題數量',
    chinese_questions INT NOT NULL DEFAULT 0 COMMENT '中文問題數量',
    status ENUM('success', 'error') NOT NULL COMMENT '執行狀態',
    error_message TEXT COMMENT '錯誤訊息',
    duration_ms INT COMMENT '執行時間(毫秒)',
    output_file VARCHAR(500) COMMENT 'S3輸出檔案路徑',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '記錄創建時間',
    INDEX idx_start_time (start_time),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='爬蟲執行記錄表';

-- 創建一些有用的視圖
-- 1. 最新問題視圖
CREATE OR REPLACE VIEW v_latest_questions AS
SELECT 
    q.id,
    q.question_id,
    q.title,
    q.language,
    q.view_count,
    q.vote_count,
    q.answers_count,
    q.has_accepted_answer,
    q.posted_at,
    GROUP_CONCAT(t.name ORDER BY t.name SEPARATOR ', ') as tags
FROM questions q
LEFT JOIN question_tags qt ON q.id = qt.question_id
LEFT JOIN tags t ON qt.tag_id = t.id
GROUP BY q.id
ORDER BY q.posted_at DESC;

-- 2. 標籤統計視圖
CREATE OR REPLACE VIEW v_tag_statistics AS
SELECT 
    t.id,
    t.name,
    COUNT(qt.question_id) as question_count,
    COUNT(CASE WHEN q.language = 'en' THEN 1 END) as english_count,
    COUNT(CASE WHEN q.language = 'zh-Hant' THEN 1 END) as chinese_count
FROM tags t
LEFT JOIN question_tags qt ON t.id = qt.tag_id
LEFT JOIN questions q ON qt.question_id = q.id
GROUP BY t.id, t.name
ORDER BY question_count DESC;

-- 3. 爬蟲執行統計視圖
CREATE OR REPLACE VIEW v_crawler_statistics AS
SELECT 
    DATE(start_time) as crawl_date,
    COUNT(*) as total_executions,
    SUM(questions_processed) as total_questions,
    SUM(english_questions) as total_english,
    SUM(chinese_questions) as total_chinese,
    AVG(duration_ms) as avg_duration_ms,
    COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count
FROM crawler_executions
GROUP BY DATE(start_time)
ORDER BY crawl_date DESC;

-- 授予權限（如果需要）
-- GRANT ALL PRIVILEGES ON repost_crawler.* TO 'your_username'@'%';
-- FLUSH PRIVILEGES;