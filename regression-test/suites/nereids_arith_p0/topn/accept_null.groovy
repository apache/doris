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

suite ("accept_null") {
   sql """ drop table IF EXISTS detail_tmp;"""

    sql """
            CREATE TABLE `detail_tmp` (
            `id` VARCHAR(512) NOT NULL,
            `accident_no` VARCHAR(512) NULL,
            `accident_type_name` VARCHAR(512) NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "min_load_replica_num" = "-1",
            "is_being_synced" = "false",
            "storage_medium" = "hdd",
            "storage_format" = "V2",
            "inverted_index_storage_format" = "V1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "false",
            "enable_single_replica_compaction" = "false",
            "group_commit_interval_ms" = "10000",
            "group_commit_data_bytes" = "134217728",
            "enable_mow_light_delete" = "false"
            );
        """

    sql "insert into detail_tmp(id,accident_type_name,accident_no) select e1,'dd',e1 from (select 1 k1) as t lateral view explode_numbers(100000) tmp1 as e1;"
    sql "delete from detail_tmp where accident_no <100;"

   def tablets = sql_return_maparray """ show tablets from detail_tmp; """

   // before full compaction, there are 7 rowsets in all tablets.
   for (def tablet : tablets) {
      int rowsetCount = 0
      def (code, out, err) = curl("GET", tablet.CompactionStatus)
      logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
      assertEquals(code, 0)
      def tabletJson = parseJson(out.trim())
      assert tabletJson.rowsets instanceof List
   }

   // trigger full compactions for all tablets by table id in ${tableName}
   def backendId_to_backendIP = [:]
   def backendId_to_backendHttpPort = [:]
   getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
   boolean disableAutoCompaction = true
   for(int i=0;i<backendId_to_backendIP.keySet().size();i++){
      backend_id = backendId_to_backendIP.keySet()[i]
      def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
      logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
      assertEquals(code, 0)
      def configList = parseJson(out.trim())
      assert configList instanceof List

      for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
               disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
            }
      }
   }

   for (def tablet : tablets) {
      String tablet_id = tablet.TabletId
      def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
      logger.info("tablet"+tablet_info)
      def table_id = tablet_info[0].TableId
      backend_id = tablet.BackendId
      def times = 1
      def code, out, err
      do{
            (code, out, err) = be_run_full_compaction_by_table_id(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), table_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            ++times
            sleep(2000)
      } while (parseJson(out.trim()).status.toLowerCase()!="success" && times<=10)

      def compactJson = parseJson(out.trim())
      if (compactJson.status.toLowerCase() == "fail") {
            assertEquals(disableAutoCompaction, false)
            logger.info("Compaction was done automatically!")
      }
      if (disableAutoCompaction) {
            assertEquals("success", compactJson.status.toLowerCase())
      }
   }

    qt_test "select id,accident_type_name,accident_no,__DORIS_DELETE_SIGN__ From detail_tmp where accident_type_name = 'dd'  order by accident_no,id limit 10;"
}
