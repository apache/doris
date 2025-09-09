create database if not exists demo.test_db;
use demo.test_db;

CREATE TABLE sc_drop_add_orc (
    id BIGINT,
    name STRING,
    age INT
)
USING iceberg
PARTITIONED BY (id)
TBLPROPERTIES ('format'='orc');

INSERT INTO sc_drop_add_orc VALUES (1, 'Alice', 25);
INSERT INTO sc_drop_add_orc VALUES (2, 'Bob', 30);

ALTER TABLE sc_drop_add_orc DROP COLUMN age;

INSERT INTO sc_drop_add_orc (id, name) VALUES (3, 'Charlie');
INSERT INTO sc_drop_add_orc (id, name) VALUES (4, 'David');

ALTER TABLE sc_drop_add_orc ADD COLUMN age INT;

INSERT INTO sc_drop_add_orc VALUES (5, 'Eve', 28);
INSERT INTO sc_drop_add_orc VALUES (6, 'Frank', 35);



CREATE TABLE sc_drop_add_parquet (
    id BIGINT,
    name STRING,
    age INT
)
USING iceberg
PARTITIONED BY (id)
TBLPROPERTIES ('format'='parquet');

INSERT INTO sc_drop_add_parquet VALUES (1, 'Alice', 25);
INSERT INTO sc_drop_add_parquet VALUES (2, 'Bob', 30);

ALTER TABLE sc_drop_add_parquet DROP COLUMN age;

INSERT INTO sc_drop_add_parquet (id, name) VALUES (3, 'Charlie');
INSERT INTO sc_drop_add_parquet (id, name) VALUES (4, 'David');

ALTER TABLE sc_drop_add_parquet ADD COLUMN age INT;

INSERT INTO sc_drop_add_parquet VALUES (5, 'Eve', 28);
INSERT INTO sc_drop_add_parquet VALUES (6, 'Frank', 35);