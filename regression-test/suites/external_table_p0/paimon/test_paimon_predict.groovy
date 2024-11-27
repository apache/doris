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

suite("test_paimon_predict", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test")
        return
    }

    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog_name = "test_paimon_predict"
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
                'type' = 'paimon',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.path.style.access' = 'true'
        );
    """
    sql """use `${catalog_name}`.`spark_paimon`"""

    explain {
        sql("select * from predict_for_in")
        contains("inputSplitNum=9")
    }

    def explain_one_column = { col_name ->

        explain {
            sql("select * from predict_for_in where ${col_name} in ('a')")
            contains("inputSplitNum=3")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} in ('b')")
            contains("inputSplitNum=3")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} in ('a','b')")
            contains("inputSplitNum=6")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} in ('a','x')")
            contains("inputSplitNum=3")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} in ('x','y')")
            contains("inputSplitNum=0")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} in ('a','b','c')")
            contains("inputSplitNum=9")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} in ('y','x','a','c')")
            contains("inputSplitNum=6")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} not in ('y','x','a','c')")
            contains("inputSplitNum=3")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} not in ('a')")
            contains("inputSplitNum=6")
        }

        explain {
            sql("select * from predict_for_in where ${col_name} not in ('x')")
            contains("inputSplitNum=9")
        }
    }

    explain_one_column('dt')
    explain_one_column('hh')


    sql """ drop catalog if exists ${catalog_name} """
}


/*

for spark:

create table predict_for_in(id int, dt string, hh string) partitioned by(dt,hh);

insert into predict_for_in values (1, 'a', 'a');
insert into predict_for_in values (2, 'a', 'b');
insert into predict_for_in values (3, 'a', 'c');

insert into predict_for_in values (4, 'b', 'a');
insert into predict_for_in values (5, 'b', 'b');
insert into predict_for_in values (6, 'b', 'c');

insert into predict_for_in values (7, 'c', 'a');
insert into predict_for_in values (8, 'c', 'b');
insert into predict_for_in values (9, 'c', 'c');

*/

