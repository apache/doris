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

suite("test_hudi_schema_evolution", "p2,external,hudi,external_remote,external_remote_hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHudiTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable hudi test")
    }

    String catalog_name = "test_hudi_schema_evolution"
    String props = context.config.otherConfigs.get("hudiEmrCatalog")
    sql """drop catalog if exists ${catalog_name};"""
    sql """
        create catalog if not exists ${catalog_name} properties (
            ${props}
        );
    """

    sql """ switch ${catalog_name};"""
    sql """ use regression_hudi;""" 
    sql """ set enable_fallback_to_original_planner=false """
    
    qt_adding_simple_columns_table """ select * from adding_simple_columns_table order by id """
    qt_altering_simple_columns_table """ select * from altering_simple_columns_table order by id """
    // qt_deleting_simple_columns_table """ select * from deleting_simple_columns_table order by id """
    // qt_renaming_simple_columns_table """ select * from renaming_simple_columns_table order by id """

    qt_adding_complex_columns_table """ select * from adding_complex_columns_table order by id """
    qt_altering_complex_columns_table """ select * from altering_complex_columns_table order by id """
    // qt_deleting_complex_columns_table """ select * from deleting_complex_columns_table order by id """
    // qt_renaming_complex_columns_table """ select * from renaming_complex_columns_table order by id """
    
    // disable jni scanner because the old hudi jni reader based on spark can't read the emr hudi data
    // sql """set force_jni_scanner = true;"""
    // qt_adding_simple_columns_table """ select * from adding_simple_columns_table order by id """
    // qt_altering_simple_columns_table """ select * from altering_simple_columns_table order by id """
    // qt_deleting_simple_columns_table """ select * from deleting_simple_columns_table order by id """
    // qt_renaming_simple_columns_table """ select * from renaming_simple_columns_table order by id """

    // qt_adding_complex_columns_table """ select * from adding_complex_columns_table order by id """
    // qt_altering_complex_columns_table """ select * from altering_complex_columns_table order by id """
    // qt_deleting_complex_columns_table """ select * from deleting_complex_columns_table order by id """
    // qt_renaming_complex_columns_table """ select * from renaming_complex_columns_table order by id """
    // sql """set force_jni_scanner = false;"""

    sql """drop catalog if exists ${catalog_name};"""
}
