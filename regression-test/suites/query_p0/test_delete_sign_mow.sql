DROP TABLE IF EXISTS delete_sign_test_mow;

CREATE TABLE IF NOT EXISTS delete_sign_test_mow (
                `uid` BIGINT NULL,
                `v1` BIGINT NULL 
                )
UNIQUE KEY(uid)
DISTRIBUTED BY HASH(uid) BUCKETS 3
PROPERTIES (
    "enable_unique_key_merge_on_write" = "true",
    "replication_num" = "1"
);

insert into delete_sign_test_mow values(1, 1);
select count(uid) from delete_sign_test_mow;
select count(distinct uid) from delete_sign_test_mow;

insert into delete_sign_test_mow values(1, 1);
select count(uid) from delete_sign_test_mow;
select count(distinct uid) from delete_sign_test_mow;

insert into delete_sign_test_mow values(2, 1);
select count(uid) from delete_sign_test_mow;
select count(distinct uid) from delete_sign_test_mow;

insert into delete_sign_test_mow(uid, v1, __DORIS_DELETE_SIGN__) values(1, 1, 1);
select count(uid) from delete_sign_test_mow;
select count(distinct uid) from delete_sign_test_mow;

insert into delete_sign_test_mow(uid, v1, __DORIS_DELETE_SIGN__) values(2, 1, 1);
select count(uid) from delete_sign_test_mow;
select count(distinct uid) from delete_sign_test_mow;
