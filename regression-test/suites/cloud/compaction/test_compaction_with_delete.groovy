import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction_with_delete") {
    def tableName = "test_compaction_with_delete"

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

        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `score` int(11) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(`id`, `name`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1;
        """

        //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
        def tablet = (sql """ show tablets from ${tableName}; """)[0]

        def triggerCompaction = { be_host, be_http_port, compact_type ->
            // trigger compactions for all tablets in ${tableName}
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X POST http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run?tablet_id=")
            sb.append(tablet_id)
            sb.append("&compact_type=${compact_type}")

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
            out = process.getText()
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            return out
        } 
        def waitForCompaction = { be_host, be_http_port ->
            // wait for all compactions done
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
                out = process.getText()
                logger.info("Get compaction status: code=" + code + ", out=" + out)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }

        sql """ use @regression_cluster_name0; """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ DELETE FROM ${tableName} WHERE id = 1; """ // [3-3]
        sql """ INSERT INTO ${tableName} VALUES (2, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (3, "a", 100); """
        sql """ DELETE FROM ${tableName} WHERE id = 2; """ // [10-10]
        // no suitable version(only [2-2]) but promote cumulative point to 4
        assertTrue(triggerCompaction(ipList[0], httpPortList[0], "cumulative").contains("-2000"));
        // TODO(cyx): check cumulative point
        // after promoting cumulative point, cumulative compaction MUST be success, and promote cumulative point to 10
        assertTrue(triggerCompaction(ipList[0], httpPortList[0], "cumulative").contains("Success"));
        waitForCompaction(ipList[0], httpPortList[0])
        qt_select_default """ SELECT * FROM ${tableName}; """
        assertTrue(triggerCompaction(ipList[0], httpPortList[0], "base").contains("Success"));
        waitForCompaction(ipList[0], httpPortList[0])
        qt_select_default """ SELECT * FROM ${tableName}; """
    } finally {
        updateBeConf(ipList[0], httpPortList[0], "disable_auto_compaction", "false");
    }
}
