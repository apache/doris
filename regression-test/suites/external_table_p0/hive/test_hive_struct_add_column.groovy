// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.



suite("test_hive_struct_add_column", "all_types,p0,external,hive,external_docker,external_docker_hive") {


    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String hivePrefix  ="hive3";
        setHivePrefix(hivePrefix)
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String hmsPort = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")

        String catalog_name = "test_hive_struct_add_column"
        sql """drop catalog if exists ${catalog_name};"""
        sql """
        create catalog if not exists ${catalog_name} properties (
            'type'='hms',
            'hadoop.username' = 'hadoop',
            'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}'
        );
        """

        sql """use `${catalog_name}`.`default`"""

        qt_desc  """ desc test_hive_struct_add_column_orc;"""
        qt_test_1  """ select * from test_hive_struct_add_column_orc order by id;"""
        qt_test_2  """ select * from test_hive_struct_add_column_orc where id = 1  order by id;"""
        qt_test_3  """ select * from test_hive_struct_add_column_orc where complex is null order by id;"""
        qt_test_4  """ select * from test_hive_struct_add_column_orc where complex is not null order by id"""
        qt_test_5  """ select complex from test_hive_struct_add_column_orc where complex is null order by id """
        qt_test_6  """ select complex from test_hive_struct_add_column_orc where complex is not null order by id  """
        qt_test_7  """select complex from test_hive_struct_add_column_orc where complex is null  order by id; """
        qt_test_8  """select complex from test_hive_struct_add_column_orc where complex is not null order by id;"""
        qt_test_9  """select sex  from test_hive_struct_add_column_orc where sex = 0 order by id;"""
        qt_test_10  """select sex  from test_hive_struct_add_column_orc where sex = 1 order by id;"""
        qt_test_11  """select sex  from test_hive_struct_add_column_orc where sex = 2  order by id;"""
        qt_test_12  """select *    from test_hive_struct_add_column_orc where sex = 2 order by id; """
        qt_test_13  """select *    from test_hive_struct_add_column_orc where id =sex  order by id;""" 
        qt_test_14  """select *    from test_hive_struct_add_column_orc where id -52=sex order by id;"""
        qt_test_15  """select *,complex[1]    from test_hive_struct_add_column_orc where struct_element(complex[1],1) = 1 order by id;"""
        qt_test_16  """ select complex    from test_hive_struct_add_column_orc where struct_element(complex[1],1) = 2 and struct_element(complex[1],2) is null  order by id ; """ 
        qt_test_17  """select details    from test_hive_struct_add_column_orc where struct_element(details,1) = 25 and struct_element(details,4) is not null  order by id;"""
        qt_test_18  """select details    from test_hive_struct_add_column_orc where struct_element(details,1) = 25 and struct_element(details,4) is  null  order by id;"""
        qt_test_19  """ select details,id    from test_hive_struct_add_column_orc where struct_element(details,1) = 25 and struct_element(details,4) is not  null order by id ;"""
        qt_test_20  """ select details,id    from test_hive_struct_add_column_orc where struct_element(details,1) = 25 and struct_element(details,4) is null order by id;"""
        qt_test_21  """ select sex,count(*)    from test_hive_struct_add_column_orc group by  sex order by count(*);"""



        qt_desc  """ desc test_hive_struct_add_column_parquet;"""
        qt_test_1  """ select * from test_hive_struct_add_column_parquet order by id;"""
        qt_test_2  """ select * from test_hive_struct_add_column_parquet where id = 1  order by id;"""
        qt_test_3  """ select * from test_hive_struct_add_column_parquet where complex is null order by id;"""
        qt_test_4  """ select * from test_hive_struct_add_column_parquet where complex is not null order by id"""
        qt_test_5  """ select complex from test_hive_struct_add_column_parquet where complex is null order by id """
        qt_test_6  """ select complex from test_hive_struct_add_column_parquet where complex is not null order by id  """
        qt_test_7  """select complex from test_hive_struct_add_column_parquet where complex is null  order by id; """
        qt_test_8  """select complex from test_hive_struct_add_column_parquet where complex is not null order by id;"""
        qt_test_9  """select sex  from test_hive_struct_add_column_parquet where sex = 0 order by id;"""
        qt_test_10  """select sex  from test_hive_struct_add_column_parquet where sex = 1 order by id;"""
        qt_test_11  """select sex  from test_hive_struct_add_column_parquet where sex = 2  order by id;"""
        qt_test_12  """select *    from test_hive_struct_add_column_parquet where sex = 2 order by id; """
        qt_test_13  """select *    from test_hive_struct_add_column_parquet where id =sex  order by id;""" 
        qt_test_14  """select *    from test_hive_struct_add_column_parquet where id -52=sex order by id;"""
        qt_test_15  """select *,complex[1]    from test_hive_struct_add_column_parquet where struct_element(complex[1],1) = 1 order by id;"""
        qt_test_16  """ select complex    from test_hive_struct_add_column_parquet where struct_element(complex[1],1) = 2 and struct_element(complex[1],2) is null  order by id ; """ 
        qt_test_17  """select details    from test_hive_struct_add_column_parquet where struct_element(details,1) = 25 and struct_element(details,4) is not null  order by id;"""
        qt_test_18  """select details    from test_hive_struct_add_column_parquet where struct_element(details,1) = 25 and struct_element(details,4) is  null  order by id;"""
        qt_test_19  """ select details,id    from test_hive_struct_add_column_parquet where struct_element(details,1) = 25 and struct_element(details,4) is not  null order by id ;"""
        qt_test_20  """ select details,id    from test_hive_struct_add_column_parquet where struct_element(details,1) = 25 and struct_element(details,4) is null order by id;"""
        qt_test_21  """ select sex,count(*)    from test_hive_struct_add_column_parquet group by  sex order by count(*);"""



        sql """drop catalog if exists ${catalog_name}"""
    }
}

/*
drop table user_info_orc;
CREATE TABLE user_info_orc (
    id INT,
    name STRING,
    details STRUCT<age:INT, city:STRING>
)
stored as orc;
INSERT INTO TABLE user_info_orc
VALUES
    (1, 'Alice', named_struct('age', 25, 'city', 'New York')),
    (2, 'Blice', named_struct('age', 26, 'city', 'New York New York')),
    (3, 'Clice', named_struct('age', 27, 'city', 'New York New York New York')),
    (4, 'Dlice', named_struct('age', 28, 'city', 'New York New York New York New York')),
    (5, 'Elice', named_struct('age', 29, 'city', 'New York New York New York New York New York'));
ALTER TABLE user_info_orc CHANGE COLUMN details details STRUCT<age:INT, city:STRING, email:STRING>;
INSERT INTO TABLE user_info_orc
VALUES
    (11, 'AAlice', named_struct('age', 125, 'city', 'acity', 'email', 'alice@example.com')),
    (12, 'BBlice', named_struct('age', 126, 'city', 'bcity', 'email', 'bob@example.com')),
    (13, 'CClice', named_struct('age', 127, 'city', 'ccity', 'email', 'alicebob@example.com')),
    (14, 'DDlice', named_struct('age', 128, 'city', 'dcity', 'email', 'xxxxxbob@example.com')),
    (15, 'EElice', named_struct('age', 129, 'city', 'ecity', 'email', NULL));
ALTER TABLE user_info_orc CHANGE COLUMN details details STRUCT<age:INT, city:STRING, email:STRING, phone:int>;
INSERT INTO  user_info_orc
VALUES
    (21, 'Charlie', named_struct('age', 218, 'city', 'San Francisco', 'email', 'asdacharlie@example.com','phone',123)),
    (22, 'Charlie', named_struct('age', 228, 'city', 'San-Francisco', 'email', 'ssscharlie@example.com','phone',1234)),
    (23, 'Charlie', named_struct('age', 238, 'city', 'SanxFrancisco', 'email', '333charlie@example.com','phone',12345)),
    (24, 'Charlie', named_struct('age', 248, 'city', 'San888Francisco', 'email', '777charlie@example.com','phone',123456)),
    (25, 'Charlie', named_struct('age', 258, 'city', 'San0000Francisco', 'email', '9999chasasrlie@example.com','phone',NULL));
desc user_info_orc;
ALTER TABLE user_info_orc add columns (sex  int);
INSERT INTO TABLE user_info_orc
VALUES
    (31, 'Alice', named_struct('age', 25, 'city', 'New York', 'email', 'alice@example.com', 'phone', 123456),0),
    (32, 'Bob', named_struct('age', 30, 'city', 'Los Angeles', 'email', 'bob@example.com', 'phone', 789012),0),
    (33, 'Charlie', named_struct('age', 28, 'city', 'San Francisco', 'email', 'charlie@example.com', 'phone', 456789),1),
    (34, 'David', named_struct('age', 32, 'city', 'Chicago', 'email', 'david@example.com', 'phone', 987654),0),
    (35, 'Eve', named_struct('age', 27, 'city', 'Seattle', 'email', 'eve@example.com', 'phone', NULL),NULL);
ALTER TABLE user_info_orc add columns (complex array<struct<a:int>>);
INSERT INTO TABLE user_info_orc
VALUES
    (41,'Alice', named_struct('age', 25, 'city', 'New York', 'email', 'alice@example.com', 'phone', 123456), 1, array(named_struct('a', 1),named_struct('a', 1))),
    (42,'Bob', named_struct('age', 30, 'city', 'Los Angeles', 'email', 'bob@example.com', 'phone', 789012), 1, array(named_struct('a', 2),named_struct('a', 1))),
    (43,'Charlie', named_struct('age', 28, 'city', 'San Francisco', 'email', 'charlie@example.com', 'phone', 456789), 2, array(named_struct('a', 3),named_struct('a', 1))),
    (44,'David', named_struct('age', 32, 'city', 'Chicago', 'email', 'david@example.com', 'phone', 987654), 1, array(named_struct('a', 4),named_struct('a', 1))),
    (45,'Eve', named_struct('age', 27, 'city', 'Seattle', 'email', 'eve@example.com', 'phone', 654321), 2, array(named_struct('a', 5),named_struct('a', 1)));

ALTER TABLE user_info_orc CHANGE COLUMN complex complex array<struct<a:int,b:struct<aa:string,bb:int>>>;
INSERT INTO TABLE user_info_orc
VALUES
    (51, 'Alice', named_struct('age', 25, 'city', 'New York', 'email', 'alice@example.com', 'phone', 123456), 1, array(named_struct('a', 1, 'b', named_struct('aa', 'foo', 'bb', 100)),named_struct('a', 1, 'b', named_struct('aa', 'foo', 'bb', 100)))),
    (52, 'Bob', named_struct('age', 30, 'city', 'Los Angeles', 'email', 'bob@example.com', 'phone', 789012), 2, array(named_struct('a', 2, 'b', named_struct('aa', 'bar', 'bb', 200)))),
    (53, 'Charlie', named_struct('age', 28, 'city', 'San Francisco', 'email', 'charlie@example.com', 'phone', 456789), 1, array(named_struct('a', 3, 'b', named_struct('aa', 'baz', 'bb', 300)))),
    (54, 'David', named_struct('age', 32, 'city', 'Chicago', 'email', 'david@example.com', 'phone', 987654), 2, array(named_struct('a', 8, 'b', named_struct('aa', 'qux', 'bb', 400)))),
    (55, 'Eve', named_struct('age', 27, 'city', 'Seattle', 'email', 'eve@example.com', 'phone', 654321), 1, array(named_struct('a', 5, 'b', named_struct('aa', 'abcd', 'bb', 500)),named_struct('a', 5, 'b', named_struct('aa', 'abcdffff', 'bb', 5000)),named_struct('a', 5, 'b', named_struct('aa', 'abcdtttt', 'bb', 500000))));


cp user_info_orc/ =>  test_hive_struct_add_column_orc/

create table test_hive_struct_add_column_orc (
  `id` int,                                         
  `name` string,                                      
  `details` struct<age:int,city:string,email:string,phone:int>,                          
  `sex` int,                                         
  `complex` array<struct<a:int,b:struct<aa:string,bb:int>>>
)
STORED AS ORC;
LOCATION '/user/doris/preinstalled_data/orc_table/test_hive_struct_add_column_orc';

*/