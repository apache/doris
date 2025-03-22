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

suite("test_parquet_complex_cross_page", "p2,external,hive,external_remote,external_remote_hive") {

    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    //hudi hive use same catalog in p2.
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable test")
        return;
    }

    String props = context.config.otherConfigs.get("hudiEmrCatalog")    
    String hms_catalog_name = "test_parquet_complex_cross_page"

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

    qt_1 """  SELECT *  FROM test_parquet_complex_cross_page WHERE device_id='DZ692'  and format_time between 1737693770300 and 1737693770500 
    and date between '20250124' and '20250124'  and project='GA20230001' ; """ 
    qt_2 """ SELECT functions_pnc_ssm_road_di_objects from test_parquet_complex_cross_page ; """ 
    qt_3 """ select * from test_parquet_complex_cross_page ; """ 
    
    sql """drop catalog ${hms_catalog_name};"""
}
