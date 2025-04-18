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

suite("test_orc_merge_io_input_streams", "p2,external,hive,external_remote,external_remote_hive") {

    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    //hudi hive use same catalog in p2.
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable test")
        return;
    }

    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    String hms_catalog_name = "test_orc_merge_io_input_streams"

    sql """drop catalog if exists ${hms_catalog_name};"""
    sql """
        CREATE CATALOG IF NOT EXISTS ${hms_catalog_name}
        PROPERTIES (
            ${props}
            ,'hive.version' = '3.1.3'
        );
    """

    logger.info("catalog " + hms_catalog_name + " created")
    sql """switch ${hms_catalog_name};"""
    logger.info("switched to catalog " + hms_catalog_name)
    sql """ use regression;"""

    sql """ set dry_run_query=true; """

    qt_1 """  SELECT trace_id as trace_id_sub,created_time FROM test_orc_merge_io_input_streams_table
              where dt = replace(date_sub('2025-04-16', 1), '-', '') and trace_id='1210647803'; """
    qt_2 """ select * from test_orc_merge_io_input_streams_table ; """

    sql """drop catalog ${hms_catalog_name};"""
}

