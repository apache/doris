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

suite("test_iceberg_partition_evolution", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_partition_evolution"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    logger.info("catalog " + catalog_name + " created")
    sql """switch ${catalog_name};"""
    logger.info("switched to catalog " + catalog_name)
    sql """ use test_db;""" 

    def tbs = sql """ show tables  """ 
    logger.info("tables = " +tbs )


    qt_add_partition_1  """desc iceberg_add_partition;"""
    qt_add_partition_2  """select * from iceberg_add_partition order by id;"""
    qt_add_partition_3  """select id from iceberg_add_partition order by id;"""
    qt_add_partition_4  """select id,name from iceberg_add_partition order by id;"""
    qt_add_partition_5  """select age from iceberg_add_partition order by id;"""
    qt_add_partition_6  """select address from iceberg_add_partition where address is not null order by id;"""
    qt_add_partition_7  """select id,name,address from iceberg_add_partition where length(address) > 5 order by id;"""
    qt_add_partition_8  """select id,name,address from iceberg_add_partition where length(address) < 5 order by id;"""
    qt_add_partition_9  """select * from iceberg_add_partition  where age > 30 order by id;"""
    qt_add_partition_10  """select * from iceberg_add_partition  where id < 5 order by id;"""

    qt_drop_partition_1  """desc iceberg_drop_partition;"""
    qt_drop_partition_2  """select * from iceberg_drop_partition order by id;"""
    qt_drop_partition_3  """select id,name,created_date from iceberg_drop_partition order by id;"""
    qt_drop_partition_4  """select * from iceberg_drop_partition where id > 5 order by id;"""
    qt_drop_partition_5  """select * from iceberg_drop_partition where id < 3 order by id;"""
    qt_drop_partition_6  """select * from iceberg_drop_partition where created_date > "2024-12-01" order by id;"""
    qt_drop_partition_7  """select * from iceberg_drop_partition where created_date > "2023-12-02" order by id;"""
    qt_drop_partition_8  """select * from iceberg_drop_partition where created_date < "2024-12-02" order by id;"""
    qt_drop_partition_9  """select * from iceberg_drop_partition where created_date <= "2024-12-03" order by id;"""
    qt_drop_partition_10  """select * from iceberg_drop_partition where created_date >= "2024-07-03" order by id;"""

    qt_replace_partition_1  """desc iceberg_replace_partition;"""
    qt_replace_partition_2  """select * from iceberg_replace_partition order by id;"""
    qt_replace_partition_3  """select id,name,created_date from iceberg_replace_partition order by id;"""
    qt_replace_partition_4  """select * from iceberg_replace_partition where id > 5 order by id;"""
    qt_replace_partition_5  """select * from iceberg_replace_partition where id < 3 order by id;"""
    qt_replace_partition_6  """select * from iceberg_replace_partition where created_date > "2024-12-01" order by id;"""
    qt_replace_partition_7  """select * from iceberg_replace_partition where created_date > "2023-12-02" order by id;"""
    qt_replace_partition_8  """select * from iceberg_replace_partition where created_date < "2024-12-02" order by id;"""
    qt_replace_partition_9  """select * from iceberg_replace_partition where created_date <= "2024-12-03" order by id;"""
    qt_replace_partition_10  """select * from iceberg_replace_partition where created_date >= "2024-07-03" order by id;"""
    qt_replace_partition_11  """select * from iceberg_replace_partition where created_date >= "2025-09-23" order by id;"""




    try {
        sql """ select * from iceberg_evolution_partition """ 
    }catch (Exception e) {
        assertTrue(e.getMessage().contains("Unable to read Iceberg table with dropped old partition column."), e.getMessage())
    }

}
/*


CREATE TABLE iceberg_add_partition (
    id INT,
    name STRING,
    age INT
) USING iceberg;
INSERT INTO iceberg_add_partition VALUES(1, 'Alice', 30),(2, 'Bob', 25);
ALTER TABLE iceberg_add_partition ADD PARTITION FIELD age;
INSERT INTO iceberg_add_partition VALUES (3, 'Diana', 30, '456');
ALTER TABLE iceberg_add_partition ADD COLUMNS address STRING;
INSERT INTO iceberg_add_partition VALUES (4, 'Charlie', 45, '123 Street Name');
ALTER TABLE iceberg_add_partition ADD PARTITION FIELD bucket(10, id);
INSERT INTO iceberg_add_partition VALUES (5, 'Eve', 29, '789 Third St');
ALTER TABLE iceberg_add_partition ADD PARTITION FIELD truncate(5, address);
INSERT INTO iceberg_add_partition VALUES (6, 'Frank', 33,"xx"),(7, 'Grace', 28,"yyyyyyyyy");


CREATE TABLE iceberg_drop_partition (
    id INT,
    name STRING,
    amount DOUBLE,
    created_date DATE
) 
USING iceberg
PARTITIONED BY (year(created_date),bucket(10,created_date));
INSERT INTO iceberg_drop_partition VALUES
    (1, 'Alice', 100.0, DATE '2023-12-01'),
    (2, 'Bob', 200.0, DATE '2023-12-02'),
    (3, 'Charlie', 300.0, DATE '2024-12-03');
ALTER TABLE iceberg_drop_partition  DROP PARTITION FIELD year(created_date);
INSERT INTO iceberg_drop_partition VALUES
    (4, 'David', 400.0, DATE '2023-12-02'),
    (5, 'Eve', 500.0, DATE '2024-12-03');
ALTER TABLE iceberg_drop_partition  DROP PARTITION FIELD bucket(10,created_date);
INSERT INTO iceberg_drop_partition VALUES
    (6, 'David', 400.0, DATE '2025-12-12'),
    (7, 'Eve', 500.0, DATE '2025-12-23');


CREATE TABLE iceberg_replace_partition (
    id INT,
    name STRING,
    amount DOUBLE,
    created_date DATE
)
USING iceberg
PARTITIONED BY (year(created_date),bucket(10,created_date));
INSERT INTO iceberg_replace_partition VALUES
    (1, 'Alice', 100.0, DATE '2023-01-01'),
    (2, 'Bob', 200.0, DATE '2023-12-02'),
    (3, 'Charlie', 300.0, DATE '2024-12-03');
ALTER TABLE iceberg_replace_partition  REPLACE PARTITION FIELD year(created_date) WITH month(created_date);
INSERT INTO iceberg_replace_partition VALUES
    (4, 'David', 400.0, DATE '2023-12-02'),
    (5, 'Eve', 500.0, DATE '2024-07-03');
ALTER TABLE iceberg_replace_partition  REPLACE PARTITION FIELD bucket(10,created_date) WITH bucket(10,id);
INSERT INTO iceberg_replace_partition VALUES
    (6, 'David', 400.0, DATE '2025-10-12'),
    (7, 'Eve', 500.0, DATE '2025-09-23');

CREATE TABLE iceberg_evolution_partition (
    id INT,
    name STRING,
    age INT
) USING iceberg;
INSERT INTO iceberg_evolution_partition VALUES(1, 'Alice', 30),(2, 'Bob', 25);
ALTER TABLE iceberg_evolution_partition ADD PARTITION FIELD age;
INSERT INTO iceberg_evolution_partition VALUES (3, 'Diana', 30, '456');
ALTER TABLE iceberg_evolution_partition ADD COLUMNS address STRING;
INSERT INTO iceberg_evolution_partition VALUES (4, 'Charlie', 45, '123 Street Name');
ALTER TABLE iceberg_evolution_partition ADD PARTITION FIELD bucket(10, id);
INSERT INTO iceberg_evolution_partition VALUES (5, 'Eve', 29, '789 Third St');
ALTER TABLE iceberg_evolution_partition REPLACE PARTITION FIELD bucket(10, id) WITH truncate(5, address);
INSERT INTO iceberg_evolution_partition VALUES (6, 'Frank', 33,"xx"),(7, 'Grace', 28,"yyyyyyyyy");
ALTER TABLE iceberg_evolution_partition DROP PARTITION FIELD truncate(5, address);
INSERT INTO iceberg_evolution_partition VALUES (8, 'Hank', 40, "zz"), (9, 'Ivy', 22, "aaaaaa");
ALTER TABLE iceberg_evolution_partition DROP COLUMNS address;
-- INSERT INTO iceberg_evolution_partition VALUES (10, 'Jack', 35), (11, 'Kara', 30);


*/


