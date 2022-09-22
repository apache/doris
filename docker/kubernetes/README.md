# deploy Doris on kubenetes
## download doris 1.1.2
```
cd docker/kubernetes
wget https://dlcdn.apache.org/doris/1.1/1.1.2-rc05/apache-doris-be-1.1.2-bin-x86_64.tar.gz
wget https://dlcdn.apache.org/doris/1.1/1.1.2-rc05/apache-doris-fe-1.1.2-bin.tar.gz
```
## build docker image
```shell
sh build_image.sh 
```
## install in kubernetes
```shell
kubectl apply -f . 
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


