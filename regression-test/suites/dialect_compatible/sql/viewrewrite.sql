--dialect option
set sql_dialect = hive;

--create catalog 
drop catalog if exists hive;
create catalog hive properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');
--test view rewrite
select * from hive.default.department_view;
select * from hive.default.department_nesting_view;



