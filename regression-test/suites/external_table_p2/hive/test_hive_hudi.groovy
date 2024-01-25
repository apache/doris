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

suite("test_hive_hudi", "p2,external,hive,hudi") {
    String enabled = context.config.otherConfigs.get("enableExternalHiveTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String extHiveHmsHost = context.config.otherConfigs.get("extHiveHmsHost")
        String extHiveHmsPort = context.config.otherConfigs.get("extHiveHmsPort")
        String catalog_name = "test_hive_hudi"

        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'hadoop.username'='hadoop',
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}'
            );
        """

        sql """use ${catalog_name}.hudi_catalog"""
        // read optimize table with partition
        qt_optimize_table """select * from partitioned_mor_ro order by rowid, versionid"""
        // copy on write table with update
        qt_merge_on_read """select * from partitioned_mor_rt order by rowid, versionid"""
        // match colum name in lower case
        qt_lowercase_column """select RoWiD, PaRtiTionID, PrEComB, VerSIonID from partitioned_mor_rt order by rowid, versionid"""

        // test complex types
        qt_complex_types """select * from complex_type_rt order by name desc limit 100"""

        // skip logs
        sql """drop catalog if exists ${catalog_name};"""
        sql """
            create catalog if not exists ${catalog_name} properties (
                'hadoop.username'='hadoop',
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${extHiveHmsHost}:${extHiveHmsPort}',
                'hoodie.datasource.merge.type'='skip_merge'
            );
        """
        // copy on write table with update, skip merge logs, so the result is the same as partitioned_mor_ro
        qt_skip_merge """select * from partitioned_mor_rt order by rowid, versionid"""

        sql """drop catalog if exists ${catalog_name};"""
    }
}
