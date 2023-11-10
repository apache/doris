-- https://github.com/apache/incubator-doris/issues/5121
DROP DATABASE IF EXISTS issue_5121;
CREATE DATABASE issue_5121;
USE issue_5121

CREATE TABLE IF NOT EXISTS doris_app_event_record (pt_d INT NULL,app VARCHAR(256) NULL, event VARCHAR(256)  NULL, distribute_id VARCHAR(256)  NULL, bitmap_id BITMAP BITMAP_UNION NULL, row_count INT  SUM NULL)PARTITION BY RANGE (pt_d) ( ) DISTRIBUTED BY HASH(distribute_id) BUCKETS 3;

SELECT pt_d FROM doris_app_event_record WHERE pt_d = 20201222;

SELECT COUNT(*) FROM doris_app_event_record WHERE pt_d = 20201222;

SELECT COUNT(pt_d) FROM doris_app_event_record WHERE pt_d = 20201222;

DROP DATABASE issue_5121;
