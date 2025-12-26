use paimon;

create database if not exists test_paimon_ro_read_db;

use test_paimon_ro_read_db;

CREATE TABLE if not exists paimon_pk_for_ro (
    id INT,
    name STRING,
    age INT
) USING paimon
TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '1');

INSERT INTO paimon_pk_for_ro (id, name, age) VALUES (1, 'Alice', 30),(2, 'Bob', 25),(3, 'Charlie', 28);

CALL sys.compact(table => 'test_paimon_ro_read_db.paimon_pk_for_ro');

INSERT INTO paimon_pk_for_ro (id, name, age) VALUES (1, 'AliceNew', 33);
