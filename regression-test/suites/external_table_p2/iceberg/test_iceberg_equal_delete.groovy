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

suite("test_iceberg_equal_delete", "p2,external,iceberg,external_remote,external_remote_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String catalog = "test_iceberg_equal_delete"
    String access_key = context.config.otherConfigs.get("dlf_access_key")
    String secret_key = context.config.otherConfigs.get("dlf_secret_key")


    sql """drop catalog if exists ${catalog};"""
    sql  """
        create catalog if not exists ${catalog} properties (
            "warehouse" = "oss://selectdb-qa-datalake-test/iceberg_temp/warehouse",
            "type" = "iceberg",
            "oss.secret_key" = "${secret_key}",
            "oss.endpoint" = "oss-cn-beijing-internal.aliyuncs.com",
            "oss.access_key" = "${access_key}",
            "iceberg.catalog.type" = "hadoop"
            ); 
    """


    sql """ use ${catalog}.flink_db """
    String tb = """ sample """

    qt_q1  """ select * from ${tb} order by id """
    qt_q2  """ select data from ${tb} where data = "sample data 8"; """
    qt_q3  """ select data from ${tb} where data = "sample data 3" """
    qt_q4  """ select * from ${tb} where id = 10 """

}
