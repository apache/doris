

use multi_catalog;


CALL system.register_table(
    table => 'multi_catalog.equality_delete_par_1',
    metadata_file => 's3a://warehouse/wh/multi_catalog/equality_delete_par_1/metadata/00011-f7268cac-dd2f-4061-a2c9-b5aa54ab52f4.metadata.json'
);

CALL system.register_table(
    table => 'multi_catalog.equality_delete_par_2',
    metadata_file => 's3a://warehouse/wh/multi_catalog/equality_delete_par_2/metadata/00011-792871f3-c5a3-4918-8c25-b57767ed9cc1.metadata.json'
);

CALL system.register_table(
    table => 'multi_catalog.equality_delete_par_3',
    metadata_file => 's3a://warehouse/wh/multi_catalog/equality_delete_par_3/metadata/00010-0264cf4e-72b2-4327-bd47-98ec8d673ecb.metadata.json'    
);


CALL system.register_table(
    table => 'multi_catalog.equality_delete_orc_1',
    metadata_file => 's3a://warehouse/wh/multi_catalog/equality_delete_orc_1/metadata/00011-568385a2-14af-4237-b186-ce46d350be19.metadata.json'
);

CALL system.register_table(
    table => 'multi_catalog.equality_delete_orc_2',
    metadata_file => 's3a://warehouse/wh/multi_catalog/equality_delete_orc_2/metadata/00011-2d06124f-e264-4cfe-822d-479afd8f3c8a.metadata.json'
);

CALL system.register_table(
    table => 'multi_catalog.equality_delete_orc_3',
    metadata_file => 's3a://warehouse/wh/multi_catalog/equality_delete_orc_3/metadata/00010-f6ba4ee7-256f-41f3-8932-25ec703d8c8b.metadata.json'
);

-- flink 
-- CREATE CATALOG iceberg_rest
--  WITH (
--    'type' = 'iceberg',
--    'catalog-type' = 'rest',
--    'uri' = 'http://127.0.0.1:18181',
--    'warehouse' = 's3://warehouse/wh',
--    's3.endpoint' = 'http://127.0.0.1:19001',
--    's3.access-key-id' = 'admin',
--    's3.secret-access-key' = 'password',
-- 'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
--    's3.path-style-access' = 'true',
--    's3.region' = 'us-east-1'
--  );
-- use iceberg_rest.daidai;

-- drop table if exists equality_delete_par_1;
-- drop table if exists equality_delete_par_2;
-- drop table if exists equality_delete_par_3;

-- SET execution.runtime-mode = batch;
-- CREATE TABLE equality_delete_par_1 (
-- `id` INT,
-- `name` string,
-- `data` STRING NOT NULL,
--     PRIMARY KEY(`id`) NOT ENFORCED
-- ) with ('format-version'='2', 'write.upsert.enabled'='true');
-- insert into equality_delete_par_1 values(1, 'smith', 'a'), (2, 'danny', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd');
-- insert into equality_delete_par_1 values(4, 'bob', 'e'), (5, 'dennis', 'f'), (6, 'jasson', 'g'), (7, 'parker', 'h');
-- alter table equality_delete_par_1 rename  id to new_id;
-- insert into equality_delete_par_1 values(1, 'smith2', 'aa'), (2, 'danny2', 'bb');
-- insert into equality_delete_par_1 values(4, 'bob2', 'ee'), (5, 'dennis2', 'ff');
-- alter table equality_delete_par_1 rename  new_id to new_new_id;
-- alter table equality_delete_par_1 rename  `name` to new_name;
-- insert into equality_delete_par_1 values(1, 'smith3', 'aaa');
-- insert into equality_delete_par_1 values(4, 'bob3', 'eee');
-- alter table equality_delete_par_1 add id int;
-- insert into equality_delete_par_1 values(1, 'smith4', 'aaaa', 1);





-- SET execution.runtime-mode = batch;
-- CREATE TABLE equality_delete_par_2 (
-- `id` INT,
-- `name` string,
-- `data` STRING NOT NULL,
--     PRIMARY KEY(`id`, `name`) NOT ENFORCED
-- ) with ('format-version'='2', 'write.upsert.enabled'='true');
-- insert into equality_delete_par_2 values(1, 'smith', 'a'), (2, 'danny', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd');
-- insert into equality_delete_par_2 values(4, 'bob', 'e'), (5, 'dennis', 'f'), (6, 'jasson', 'g'), (7, 'parker', 'h');
-- alter table equality_delete_par_2 rename  id to new_id;
-- insert into equality_delete_par_2 values(1, 'smith', 'aa'), (2, 'danny', 'bb');
-- insert into equality_delete_par_2 values(4, 'bob', 'ee'), (5, 'dennis', 'ff');
-- alter table equality_delete_par_2 rename  new_id to new_new_id;
-- alter table equality_delete_par_2 rename  `name` to new_name;
-- insert into equality_delete_par_2 values(1, 'smith', 'aaa');
-- insert into equality_delete_par_2 values(4, 'bob', 'eee');
-- alter table equality_delete_par_2 add id int;
-- insert into equality_delete_par_2 values(1, 'smith', 'aaaa', 1);



-- SET execution.runtime-mode = batch;
-- CREATE TABLE equality_delete_par_3 (
-- `id` INT not null,
-- `name` string not null,
-- `data` STRING NOT NULL,
--     PRIMARY KEY(`id`, `name`) NOT ENFORCED
-- ) with ('format-version'='2', 'write.upsert.enabled'='true');
-- insert into equality_delete_par_3 values(1, 'smith', 'a'), (2, 'danny', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd');
-- insert into equality_delete_par_3 values(4, 'bob', 'e'), (5, 'dennis', 'f'), (6, 'jasson', 'g'), (7, 'parker', 'h');
-- alter table equality_delete_par_3 rename  id to new_id;
-- insert into equality_delete_par_3 values(1, 'smith', 'aa'), (2, 'danny', 'bb');
-- insert into equality_delete_par_3 values(4, 'bob', 'ee'), (5, 'dennis', 'ff');
-- ALTER TABLE equality_delete_par_3 MODIFY (
--     `new_id` INT,`name` string,`data` STRING NOT NULL, 
--     PRIMARY KEY (new_id) NOT ENFORCED);
-- insert into equality_delete_par_3 values(1, 'smith3', 'aaa');
-- insert into equality_delete_par_3 values(4, 'bob3', 'eee');
-- ALTER TABLE equality_delete_par_3 MODIFY (
--     `new_id` INT , `name` string ,`data` STRING , 
--     PRIMARY KEY (new_id, `data`) NOT ENFORCED);
-- insert into equality_delete_par_3 values(1, 'smith4', 'aaa');




-- drop table if exists equality_delete_orc_1;
-- drop table if exists equality_delete_orc_2;
-- drop table if exists equality_delete_orc_3;

-- SET execution.runtime-mode = batch;
-- CREATE TABLE equality_delete_orc_1 (
-- `id` INT,
-- `name` string,
-- `data` STRING NOT NULL,
--     PRIMARY KEY(`id`) NOT ENFORCED
-- ) with ('format-version'='2', 'write.upsert.enabled'='true','write.format.default' = 'orc');
-- insert into equality_delete_orc_1 values(1, 'smith', 'a'), (2, 'danny', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd');
-- insert into equality_delete_orc_1 values(4, 'bob', 'e'), (5, 'dennis', 'f'), (6, 'jasson', 'g'), (7, 'orcker', 'h');
-- alter table equality_delete_orc_1 rename  id to new_id;
-- insert into equality_delete_orc_1 values(1, 'smith2', 'aa'), (2, 'danny2', 'bb');
-- insert into equality_delete_orc_1 values(4, 'bob2', 'ee'), (5, 'dennis2', 'ff');
-- alter table equality_delete_orc_1 rename  new_id to new_new_id;
-- alter table equality_delete_orc_1 rename  `name` to new_name;
-- insert into equality_delete_orc_1 values(1, 'smith3', 'aaa');
-- insert into equality_delete_orc_1 values(4, 'bob3', 'eee');
-- alter table equality_delete_orc_1 add id int;
-- insert into equality_delete_orc_1 values(1, 'smith4', 'aaaa', 1);





-- SET execution.runtime-mode = batch;
-- CREATE TABLE equality_delete_orc_2 (
-- `id` INT,
-- `name` string,
-- `data` STRING NOT NULL,
--     PRIMARY KEY(`id`, `name`) NOT ENFORCED
-- ) with ('format-version'='2', 'write.upsert.enabled'='true','write.format.default' = 'orc');
-- insert into equality_delete_orc_2 values(1, 'smith', 'a'), (2, 'danny', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd');
-- insert into equality_delete_orc_2 values(4, 'bob', 'e'), (5, 'dennis', 'f'), (6, 'jasson', 'g'), (7, 'orcker', 'h');
-- alter table equality_delete_orc_2 rename  id to new_id;
-- insert into equality_delete_orc_2 values(1, 'smith', 'aa'), (2, 'danny', 'bb');
-- insert into equality_delete_orc_2 values(4, 'bob', 'ee'), (5, 'dennis', 'ff');
-- alter table equality_delete_orc_2 rename  new_id to new_new_id;
-- alter table equality_delete_orc_2 rename  `name` to new_name;
-- insert into equality_delete_orc_2 values(1, 'smith', 'aaa');
-- insert into equality_delete_orc_2 values(4, 'bob', 'eee');
-- alter table equality_delete_orc_2 add id int;
-- insert into equality_delete_orc_2 values(1, 'smith', 'aaaa', 1);



-- SET execution.runtime-mode = batch;
-- CREATE TABLE equality_delete_orc_3 (
-- `id` INT not null,
-- `name` string not null,
-- `data` STRING NOT NULL,
--     PRIMARY KEY(`id`, `name`) NOT ENFORCED
-- ) with ('format-version'='2', 'write.upsert.enabled'='true','write.format.default' = 'orc');
-- insert into equality_delete_orc_3 values(1, 'smith', 'a'), (2, 'danny', 'b'), (3, 'alice', 'c'), (4, 'bob', 'd');
-- insert into equality_delete_orc_3 values(4, 'bob', 'e'), (5, 'dennis', 'f'), (6, 'jasson', 'g'), (7, 'orcker', 'h');
-- alter table equality_delete_orc_3 rename  id to new_id;
-- insert into equality_delete_orc_3 values(1, 'smith', 'aa'), (2, 'danny', 'bb');
-- insert into equality_delete_orc_3 values(4, 'bob', 'ee'), (5, 'dennis', 'ff');
-- ALTER TABLE equality_delete_orc_3 MODIFY (
--     `new_id` INT,`name` string,`data` STRING NOT NULL, 
--     PRIMARY KEY (new_id) NOT ENFORCED);
-- insert into equality_delete_orc_3 values(1, 'smith3', 'aaa');
-- insert into equality_delete_orc_3 values(4, 'bob3', 'eee');
-- ALTER TABLE equality_delete_orc_3 MODIFY (
--     `new_id` INT , `name` string ,`data` STRING , 
--     PRIMARY KEY (new_id, `data`) NOT ENFORCED);
-- insert into equality_delete_orc_3 values(1, 'smith4', 'aaa');