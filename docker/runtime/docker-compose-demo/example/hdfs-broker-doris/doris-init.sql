create database test;
use test;
CREATE TABLE user_basic (
    user_id int(11) NULL COMMENT "用户ID",
    user_name varchar(20) NULL COMMENT "用户姓名",
    user_msg varchar(20) NULL COMMENT "用户信息"
) ENGINE=OLAP
DUPLICATE KEY(user_id)
COMMENT "OLAP"
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES("replication_num" = "1");
