-- https://github.com/apache/incubator-doris/issues/4971
DROP DATABASE IF EXISTS issue_4971
CREATE DATABASE issue_4971
USE issue_4971
CREATE TABLE `t1` (`a` int NULL COMMENT "", `b` largeint NULL COMMENT "") AGGREGATE KEY(a, b) DISTRIBUTED BY HASH(`a`) BUCKETS 10
CREATE TABLE `t2` (`a` int NULL COMMENT "", `b` decimal(19, 6) NULL COMMENT "") AGGREGATE KEY(a, b) DISTRIBUTED BY HASH(`a`) BUCKETS 10
curl --location-trusted -u root:{FE_PASSWORD} -H "format:json" -T ./data/json_4971_1 http://{FE_HOST}:{HTTP_PORT}/api/issue_4971/t1/_stream_load
curl --location-trusted -u root:{FE_PASSWORD} -H "format:json" -H "num_as_string:true" -T ./data/json_4971_1 http://{FE_HOST}:{HTTP_PORT}/api/issue_4971/t1/_stream_load
SELECT * FROM t1 order by a, b
curl --location-trusted -u root:{FE_PASSWORD} -H "format:json" -H "num_as_string:true" -T ./data/json_4971_2 http://{FE_HOST}:{HTTP_PORT}/api/issue_4971/t2/_stream_load
SELECT * FROM t2 order by a, b
curl --location-trusted -u root:{FE_PASSWORD} -H "format:json" -T ./data/json_4971_2 http://{FE_HOST}:{HTTP_PORT}/api/issue_4971/t2/_stream_load
DROP DATABASE issue_4971
