use demo.test_db;

drop table if exists dangling_delete_after_write;
create table dangling_delete_after_write (
  id BIGINT NOT NULL,
  val STRING)
USING iceberg
TBLPROPERTIES (
  'format' = 'iceberg/parquet',
  'format-version' = '2',
  'identifier-fields' = '[id]',
  'upsert-enabled' = 'true',
  'write.delete.mode' = 'merge-on-read',
  'write.parquet.compression-codec' = 'zstd',
  'write.update.mode' = 'merge-on-read',
  'write.upsert.enabled' = 'true');

insert into dangling_delete_after_write values(1, 'abd');
update dangling_delete_after_write set val = 'def' where id = 1;
call demo.system.rewrite_data_files(table => 'demo.test_db.dangling_delete_after_write', options => map('min-input-files', '1'));
insert into dangling_delete_after_write values(2, 'xyz');