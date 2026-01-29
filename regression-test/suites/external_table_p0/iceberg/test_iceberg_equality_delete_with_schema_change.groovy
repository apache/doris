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

suite("test_iceberg_equality_delete_with_schema_change", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_equality_delete_with_schema_change"

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

    sql """switch ${catalog_name};"""
    sql """ use multi_catalog;"""

    

    for (String format: ["par","orc"]) {

    // Basic full table scan
    order_qt_q1 """ SELECT * FROM equality_delete_${format}_1 ORDER BY new_new_id; """
    order_qt_q1_limit1 """ SELECT * FROM equality_delete_${format}_1 ORDER BY new_new_id limit 1; """
    order_qt_q1_limit2 """ SELECT * FROM equality_delete_${format}_1 ORDER BY new_new_id limit 10; """
    

    // Query by primary key (new_new_id)
    order_qt_q2 """ SELECT * FROM equality_delete_${format}_1 WHERE new_new_id = 1 ORDER BY new_new_id; """
    order_qt_q3 """ SELECT * FROM equality_delete_${format}_1 WHERE new_new_id = 4 ORDER BY new_new_id; """
    
    // Query with range conditions
    order_qt_q4 """ SELECT id FROM equality_delete_${format}_1 WHERE new_new_id > 3 ORDER BY new_new_id; """
    order_qt_q5 """ SELECT new_new_id FROM equality_delete_${format}_1 WHERE new_new_id <= 2 ORDER BY new_new_id; """
    order_qt_q6 """ SELECT data FROM equality_delete_${format}_1 WHERE new_new_id BETWEEN 2 AND 5 ORDER BY new_new_id; """
    
    // Query with modulo operation
    order_qt_q7 """ SELECT * FROM equality_delete_${format}_1 WHERE new_new_id % 2 = 1 ORDER BY new_new_id; """
    order_qt_q8 """ SELECT * FROM equality_delete_${format}_1 WHERE new_new_id % 2 = 0 ORDER BY new_new_id; """
    
    // Query by name column
    order_qt_q9 """ SELECT * FROM equality_delete_${format}_1 WHERE new_name = 'smith4' ORDER BY new_new_id; """
    order_qt_q10 """ SELECT * FROM equality_delete_${format}_1 WHERE new_name LIKE 'smith%' ORDER BY new_new_id; """
    
    // Query by data column
    order_qt_q11 """ SELECT new_name FROM equality_delete_${format}_1 WHERE data < 'f' ORDER BY new_new_id; """
    order_qt_q12 """ SELECT data FROM equality_delete_${format}_1 WHERE data >= 'e' ORDER BY new_new_id; """
    
    // Query by the added id column
    order_qt_q13 """ SELECT * FROM equality_delete_${format}_1 WHERE id = 1 ORDER BY new_new_id; """
    order_qt_q14 """ SELECT * FROM equality_delete_${format}_1 WHERE id IS NOT NULL ORDER BY new_new_id; """
    
    // Aggregation queries
    order_qt_q15 """ SELECT count(*) FROM equality_delete_${format}_1; """
    order_qt_q16 """ SELECT count(new_new_id) FROM equality_delete_${format}_1; """
    order_qt_q17 """ SELECT count(new_name) FROM equality_delete_${format}_1; """
    order_qt_q18 """ SELECT count(data) FROM equality_delete_${format}_1; """
    order_qt_q19 """ SELECT count(id) FROM equality_delete_${format}_1; """
    
    // Aggregation with conditions
    order_qt_q20 """ SELECT count(*) FROM equality_delete_${format}_1 WHERE new_new_id > 4; """
    order_qt_q21 """ SELECT count(new_new_id) FROM equality_delete_${format}_1 WHERE new_new_id % 2 = 1; """
    
    // Basic full table scan
    order_qt_q22 """ SELECT * FROM equality_delete_${format}_2 ORDER BY new_new_id, new_name; """
    order_qt_q22_limit1 """ SELECT * FROM equality_delete_${format}_2 ORDER BY new_new_id, new_name limit 1; """
    order_qt_q22_limit2 """ SELECT * FROM equality_delete_${format}_2 ORDER BY new_new_id limit 2; """
    order_qt_q22_limit3 """ SELECT * FROM equality_delete_${format}_2 ORDER BY new_name limit 3; """


    // Query by primary key components
    order_qt_q23 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id = 1 ORDER BY new_new_id, new_name; """
    order_qt_q24 """ SELECT new_name FROM equality_delete_${format}_2 WHERE new_new_id = 4 ORDER BY new_new_id, new_name; """
    order_qt_q25 """ SELECT new_name FROM equality_delete_${format}_2 WHERE new_name = 'smith' order by  new_name; """
    
    // Query with composite key conditions
    order_qt_q26 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id = 1 AND new_name = 'smith' ORDER BY new_new_id, new_name; """
    order_qt_q27 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id = 4 AND new_name = 'bob' ORDER BY new_new_id, new_name; """
    
    // Query with range conditions
    order_qt_q28 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id > 3 ORDER BY new_new_id, new_name; """
    order_qt_q29 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id BETWEEN 2 AND 5 ORDER BY new_new_id, new_name; """
    
    // Query with modulo operation
    order_qt_q30 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id % 2 = 1 ORDER BY new_new_id, new_name; """
    order_qt_q31 """ SELECT * FROM equality_delete_${format}_2 WHERE new_new_id % 3 = 1 ORDER BY new_new_id, new_name; """
    
    // Query by data column
    order_qt_q32 """ SELECT data FROM equality_delete_${format}_2 WHERE data < 'f' ORDER BY new_new_id, new_name; """
    order_qt_q33 """ SELECT data FROM equality_delete_${format}_2 WHERE data LIKE 'a%' ORDER BY new_new_id, new_name; """
    
    // Query by the added id column
    order_qt_q34 """ SELECT id FROM equality_delete_${format}_2 WHERE id = 1 ORDER BY new_new_id, new_name; """
    order_qt_q35 """ SELECT id FROM equality_delete_${format}_2 WHERE id IS NOT NULL ORDER BY new_new_id, new_name; """
    
    // Aggregation queries
    order_qt_q36 """ SELECT count(*) FROM equality_delete_${format}_2; """
    order_qt_q37 """ SELECT count(new_new_id) FROM equality_delete_${format}_2; """
    order_qt_q38 """ SELECT count(new_name) FROM equality_delete_${format}_2; """
    order_qt_q39 """ SELECT count(data) FROM equality_delete_${format}_2; """
    order_qt_q40 """ SELECT count(id) FROM equality_delete_${format}_2; """
    
    // Aggregation with conditions
    order_qt_q41 """ SELECT count(*) FROM equality_delete_${format}_2 WHERE new_new_id > 4; """
    order_qt_q42 """ SELECT count(new_new_id) FROM equality_delete_${format}_2 WHERE new_new_id % 2 = 1; """
    order_qt_q43 """ SELECT count(*) FROM equality_delete_${format}_2 WHERE new_name = 'smith'; """
    
    
    // Basic full table scan
    order_qt_q44 """ SELECT * FROM equality_delete_${format}_3 ORDER BY new_id, name; """
    order_qt_q44_limit1  """ SELECT * FROM equality_delete_${format}_3 ORDER BY new_id, name limit 3; """
    order_qt_q44_limit2 """ SELECT * FROM equality_delete_${format}_3 ORDER BY new_id limit 3; """
    order_qt_q44_limit3 """ SELECT * FROM equality_delete_${format}_3 ORDER BY name limit 3;  """

    // Query by primary key (new_id)
    order_qt_q45 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id = 1 ORDER BY new_id, name; """
    order_qt_q46 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id = 4 ORDER BY new_id, name; """
    
    // Query with range conditions on primary key
    order_qt_q47 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id > 3 ORDER BY new_id, name; """
    order_qt_q48 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id <= 2 ORDER BY new_id, name; """
    order_qt_q49 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id BETWEEN 2 AND 5 ORDER BY new_id, name; """
    
    // Query with modulo operation
    order_qt_q50 """ SELECT name FROM equality_delete_${format}_3 WHERE new_id % 2 = 1 ORDER BY new_id, name; """
    order_qt_q51 """ SELECT new_id FROM equality_delete_${format}_3 WHERE new_id % 3 = 1 ORDER BY new_id, name; """
    
    // Query by name column
    order_qt_q52 """ SELECT * FROM equality_delete_${format}_3 WHERE name = 'smith' ORDER BY new_id, name; """
    order_qt_q53 """ SELECT name FROM equality_delete_${format}_3 WHERE name LIKE 'smith%' ORDER BY new_id, name; """
    order_qt_q54 """ SELECT name FROM equality_delete_${format}_3 WHERE name = 'smith4' ORDER BY new_id, name; """
    
    // Query by data column (part of final primary key)
    order_qt_q55 """ SELECT * FROM equality_delete_${format}_3 WHERE data = 'aaa' ORDER BY new_id, name; """
    order_qt_q56 """ SELECT data FROM equality_delete_${format}_3 WHERE data < 'f' ORDER BY new_id, name; """
    order_qt_q57 """ SELECT data FROM equality_delete_${format}_3 WHERE data LIKE 'a%' ORDER BY new_id, name; """
    
    // Query with composite conditions (final primary key: new_id, data)
    order_qt_q58 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id = 1 AND data = 'aaa' ORDER BY new_id, name; """
    order_qt_q59 """ SELECT * FROM equality_delete_${format}_3 WHERE new_id = 4 AND data = 'eee' ORDER BY new_id, name; """
    
    // Aggregation queries
    order_qt_q60 """ SELECT count(*) FROM equality_delete_${format}_3; """
    order_qt_q61 """ SELECT count(new_id) FROM equality_delete_${format}_3; """
    order_qt_q62 """ SELECT count(name) FROM equality_delete_${format}_3; """
    order_qt_q63 """ SELECT count(data) FROM equality_delete_${format}_3; """
    
    // Aggregation with conditions
    order_qt_q64 """ SELECT count(*) FROM equality_delete_${format}_3 WHERE new_id > 4; """
    order_qt_q65 """ SELECT count(new_id) FROM equality_delete_${format}_3 WHERE new_id % 2 = 1; """
    order_qt_q66 """ SELECT count(*) FROM equality_delete_${format}_3 WHERE name = 'smith'; """
    order_qt_q67 """ SELECT count(*) FROM equality_delete_${format}_3 WHERE data = 'aaa'; """
    order_qt_q68 """ SELECT count(*) FROM equality_delete_${format}_3 WHERE new_id = 1 AND data = 'aaa'; """
    
    // Compare counts across tables
    order_qt_q69 """ SELECT 'equality_delete_${format}_1' as table_name, count(*) as cnt FROM equality_delete_${format}_1
               UNION ALL
               SELECT 'equality_delete_${format}_2' as table_name, count(*) as cnt FROM equality_delete_${format}_2
               UNION ALL
               SELECT 'equality_delete_${format}_3' as table_name, count(*) as cnt FROM equality_delete_${format}_3; """
    
    // Query records with same new_new_id/new_id across tables
    order_qt_q70 """ SELECT new_new_id, new_name, data FROM equality_delete_${format}_1 WHERE new_new_id = 1
               UNION ALL
               SELECT new_new_id, new_name, data FROM equality_delete_${format}_2 WHERE new_new_id = 1; """
    }
}
