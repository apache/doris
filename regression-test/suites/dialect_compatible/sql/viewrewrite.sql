--dialect option
set sql_dialect = hive;

--create catalog 
drop catalog if exists hive;
create catalog hive properties('type' = 'hms', 'hive.metastore.uris' = 'thrift://10.16.10.6:9083');
--switch catalog
switch hive;
--test view rewrite
select * from default.department_view;
select * from default.department_nesting_view;



