CREATE TABLE `unsupported_type_table`(
  k1 int,
  k2 string,
  k3 double,
  k4 map<string,int>,
  k5 STRUCT<
            houseno:    STRING
           ,streetname: STRING
           >,
  k6 int
);

set hive.stats.column.autogather=false;

