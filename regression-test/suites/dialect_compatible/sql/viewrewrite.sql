--create catalog 
drop catalog if exists hive_view;
create catalog hive_view properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://192.168.0.1:9083');
--dialect option
set sql_dialect = hive;
--test view rewrite
select * from hive_view.default.department_view;
select * from hive_view.default.department_nesting_view;



