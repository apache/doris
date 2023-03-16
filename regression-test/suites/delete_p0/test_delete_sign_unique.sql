DROP TABLE IF EXISTS delete_sign_test;

CREATE TABLE IF NOT EXISTS delete_sign_test (
                `uid` BIGINT NULL,
                `v1` BIGINT NULL 
                )
UNIQUE KEY(uid)
DISTRIBUTED BY HASH(uid) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

insert into delete_sign_test values(1, 1);
select count(uid) from delete_sign_test;
select count(distinct uid) from delete_sign_test;

insert into delete_sign_test values(1, 1);
select count(uid) from delete_sign_test;
select count(distinct uid) from delete_sign_test;

insert into delete_sign_test values(2, 1);
select count(uid) from delete_sign_test;
select count(distinct uid) from delete_sign_test;

insert into delete_sign_test(uid, v1, __DORIS_DELETE_SIGN__) values(1, 1, 1);
select count(uid) from delete_sign_test;
select count(distinct uid) from delete_sign_test;

insert into delete_sign_test(uid, v1, __DORIS_DELETE_SIGN__) values(2, 1, 1);
select count(uid) from delete_sign_test;
select count(distinct uid) from delete_sign_test;

