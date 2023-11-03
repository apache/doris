-- https://github.com/apache/incubator-doris/pull/4359
set sql_mode='';
show variables like 'sql_mode';
set sql_mode = "STRICT_TRANS_TABLES";
show variables like 'sql_mode';
select @@sql_mode;
