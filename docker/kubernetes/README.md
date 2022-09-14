# deploy Doris on kubenetes

## build docker image
```shell
sh build_image.sh 
```
## register be
```shell
mysql -hdoris-fe-0.doris-fe-service -P9030 -uroot
 
```
execute sql script
```sql
ALTER SYSTEM ADD BACKEND "doris-be-0.doris-be-service:9050";
ALTER SYSTEM ADD BACKEND "doris-be-1.doris-be-service:9050";
ALTER SYSTEM ADD BACKEND "doris-be-2.doris-be-service:9050";
SHOW PROC '/backends';
```


## test sql
```sql
CREATE DATABASE example_db;

USE example_db;

CREATE TABLE table1
(
    siteid INT DEFAULT '10',
    citycode SMALLINT,
    username VARCHAR(32) DEFAULT '',
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, citycode, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10
PROPERTIES("replication_num" = "3");

insert into table1 values(1,1,"test",2);

select * from table1;
```


