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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cloud_full_compaction_by_table_id") {
    def tableName = "test_cloud_full_compaction_by_table_id"

    if (isCloudMode()) {
        try {
            def frontendipToPort = getFrontendIpHttpPort()

            sql """ DROP TABLE IF EXISTS ${tableName} """
            sql """
                CREATE TABLE ${tableName} (
                `user_id` INT NOT NULL, `value` INT NOT NULL)
                UNIQUE KEY(`user_id`) 
                DISTRIBUTED BY HASH(`user_id`) 
                BUCKETS 8 
                PROPERTIES (
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true");"""

            // version1 (1,1)(2,2)
            sql """ INSERT INTO ${tableName} VALUES
                (1,1),(2,2)
                """
            qt_1 """select * from ${tableName} order by user_id"""


            // version2 (1,10)(2,20)
            sql """ INSERT INTO ${tableName} VALUES
                (1,10),(2,20)
                """
            qt_2 """select * from ${tableName} order by user_id"""


            // version3 (1,100)(2,200)
            sql """ INSERT INTO ${tableName} VALUES
                (1,100),(2,200)
                """
            qt_3 """select * from ${tableName} order by user_id"""


            // version4 (1,100)(2,200)(3,300)
            sql """ INSERT INTO ${tableName} VALUES
                (3,300)
                """
            qt_4 """select * from ${tableName} order by user_id"""


            // version5 (1,100)(2,200)(3,100)
            sql """update ${tableName} set value = 100 where user_id = 3"""
            qt_5 """select * from ${tableName} order by user_id"""


            // version6 (1,100)(2,200)
            sql """delete from ${tableName} where user_id = 3"""
            qt_6 """select * from ${tableName} order by user_id"""

            sql "SET skip_delete_predicate = true"
            sql "SET skip_delete_sign = true"
            sql "SET skip_delete_bitmap = true"
            // show all hidden data
            // (1,10)(1,100)(2,2)(2,20)(2,200)(3,300)(3,100)
            qt_skip_delete """select * from ${tableName} order by user_id, value"""

            //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """

            // before full compaction, there are 7 rowsets in all tablets.
            for (def tablet : tablets) {
                int rowsetCount = 0
                def (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                rowsetCount =((List<String>) tabletJson.rowsets).size()
                assertEquals (rowsetCount, 7)
            }

            // trigger full compactions for all tablets by table id in ${tableName}
            def tablet = tablets[0]
            String tablet_id = tablet.TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet"+tablet_info)
            def parts = frontendipToPort[0].split(":")
            def frontendip = parts[0]
            def port = parts[1]
            def table_id = tablet_info[0].TableId
            def command = "curl -u root: " +
                " -X POST" +
                " http://${context.config.feHttpAddress}/api/compaction/run?table_id=${table_id}&compact_type=full"
            log.info("full compaction command: ${command}")

            def process = command.execute()
            code = process.waitFor()
            out = process.text
            log.info("full compaction result: ${out}".toString())

            Thread.sleep(10000)

            // after full compaction, there is only 2 rowsets.
            for (def t : tablets) {
                int rowsetCount = 0
                def (code, out, err) = curl("GET", t.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                rowsetCount =((List<String>) tabletJson.rowsets).size()
                assertEquals (rowsetCount, 2)
            }

            // make sure all hidden data has been deleted
            // (1,100)(2,200)
            qt_select_final """select * from ${tableName} order by user_id"""
        } finally {
            try_sql("DROP TABLE IF EXISTS ${tableName}")
        }

    }


}
