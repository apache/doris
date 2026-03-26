use demo.test_db;


create table if not exists test_rewrite_data_with_update (
  id INT,
  name STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);


INSERT INTO test_rewrite_data_with_update VALUES
(1, 'a'),(2, 'b'),(3, 'c');

update test_rewrite_data_with_update set name = "bb"  where id = 1;



create table if not exists test_rewrite_data_with_delete (
  id INT,
  name STRING
)
USING iceberg
TBLPROPERTIES (
  'format-version' = '2',
  'write.delete.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);


INSERT INTO test_rewrite_data_with_delete VALUES
(1, 'a'),(2, 'b'),(3, 'c');

delete from test_rewrite_data_with_delete where id = 1;