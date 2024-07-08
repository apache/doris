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

suite("test_full_compaction") {
    def tableName = "test_full_compaction"

    List<String> ipList = new ArrayList<>()
    List<String> hbPortList = new ArrayList<>()
    List<String> httpPortList = new ArrayList<>()
    List<String> beUniqueIdList = new ArrayList<>()

    String[] bes = context.config.multiClusterBes.split(',');
    println("the value is " + context.config.multiClusterBes);
    for(String values : bes) {
        println("the value is " + values);
        String[] beInfo = values.split(':');
        ipList.add(beInfo[0]);
        hbPortList.add(beInfo[1]);
        httpPortList.add(beInfo[2]);
        beUniqueIdList.add(beInfo[3]);
    }

    println("the ip is " + ipList);
    println("the heartbeat port is " + hbPortList);
    println("the http port is " + httpPortList);
    println("the be unique id is " + beUniqueIdList);

    sleep(1000)
    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    sleep(21000)

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    sleep(21000)

    result  = sql "show clusters"
    assertEquals(result.size(), 2);
    
    def updateBeConf = { backend_ip, backend_http_port, key, value ->
        String command = "curl -X POST http://${backend_ip}:${backend_http_port}/api/update_config?${key}=${value}"
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        assertEquals(code, 0)
    }

    try {
        updateBeConf(ipList[0], httpPortList[0], "disable_auto_compaction", "true");
        updateBeConf(ipList[1], httpPortList[1], "disable_auto_compaction", "true");

        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
            `user_id` INT NOT NULL, `value` INT NOT NULL)
            UNIQUE KEY(`user_id`) 
            DISTRIBUTED BY HASH(`user_id`) 
            BUCKETS 4 
            PROPERTIES ("replication_allocation" = "tag.location.default: 1",
            "enable_unique_key_merge_on_write" = "true");"""

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        String[][] tablets = sql """ show tablets from ${tableName}; """

        def doCompaction = { be_host, be_http_port, compact_type ->
            // trigger compactions for all tablets in ${tableName}
            for (String[] tablet in tablets) {
                String tablet_id = tablet[0]
                StringBuilder sb = new StringBuilder();
                sb.append("curl -X POST http://${be_host}:${be_http_port}")
                sb.append("/api/compaction/run?tablet_id=")
                sb.append(tablet_id)
                sb.append("&compact_type=${compact_type}")

                String command = sb.toString()
                process = command.execute()
                code = process.waitFor()
                err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                out = process.getText()
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactJson = parseJson(out.trim())
                assertEquals("success", compactJson.status.toLowerCase())
            }

            // wait for all compactions done
            for (String[] tablet in tablets) {
                boolean running = true
                do {
                    Thread.sleep(1000)
                    String tablet_id = tablet[0]
                    StringBuilder sb = new StringBuilder();
                    sb.append("curl -X GET http://${be_host}:${be_http_port}")
                    sb.append("/api/compaction/run_status?tablet_id=")
                    sb.append(tablet_id)

                    String command = sb.toString()
                    logger.info(command)
                    process = command.execute()
                    code = process.waitFor()
                    err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
                    out = process.getText()
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } while (running)
            }
        }

        sql """ use @regression_cluster_name0; """
        sql """ INSERT INTO ${tableName} VALUES (1,1),(2,2) """
        sql """ INSERT INTO ${tableName} VALUES (1,10),(2,20) """
        sql """ INSERT INTO ${tableName} VALUES (1,100),(2,200) """

        sql """ use @regression_cluster_name1; """
        sql """ INSERT INTO ${tableName} VALUES (3,300) """
        sql """update ${tableName} set value = 100 where user_id = 3"""
        sql """delete from ${tableName} where user_id = 3"""

        qt_all """select * from ${tableName} order by user_id"""

        sql "SET skip_delete_predicate = true"
        sql "SET skip_delete_sign = true"
        sql "SET skip_delete_bitmap = true"
        qt_skip_delete """select * from ${tableName} order by user_id, value"""

        doCompaction.call(ipList[1], httpPortList[1], "full")

        // make sure all hidden data has been deleted
        qt_select_final """select * from ${tableName} order by user_id"""
        sql """ use @regression_cluster_name0; """
        qt_select_final """select * from ${tableName} order by user_id"""


        sql "SET skip_delete_predicate = false"
        sql "SET skip_delete_sign = false"
        sql "SET skip_delete_bitmap = false"
        sql """ INSERT INTO ${tableName} VALUES (1,1),(2,2) """
        sql """ INSERT INTO ${tableName} VALUES (1,10),(2,20) """
        qt_all """select * from ${tableName} order by user_id"""

        sql "SET skip_delete_predicate = true"
        sql "SET skip_delete_sign = true"
        sql "SET skip_delete_bitmap = true"
        qt_skip_delete """select * from ${tableName} order by user_id, value"""

        doCompaction.call(ipList[0], httpPortList[0], "full")

        qt_select_final """select * from ${tableName} order by user_id"""
        sql """ use @regression_cluster_name1; """
        qt_select_final """select * from ${tableName} order by user_id"""

    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        updateBeConf(ipList[0], httpPortList[0], "disable_auto_compaction", "false");
        updateBeConf(ipList[1], httpPortList[1], "disable_auto_compaction", "false");
    }
}
