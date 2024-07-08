import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_compaction") {
    def tableName = "test_compaction"

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
    wait_cluster_change()

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    wait_cluster_change()

    result  = sql "show clusters"
    assertEquals(result.size(), 2);
    
    def updateBeConf = { backend_ip, backend_http_port, key, value ->
        String command = "curl -X POST http://${backend_ip}:${backend_http_port}/api/update_config?${key}=${value}"
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        assertEquals(code, 0)
    }
    def injectionPoint = { backend_ip, backend_http_port, args ->
        String command = "curl -X GET http://${backend_ip}:${backend_http_port}/api/injection_point/${args}"
        logger.info(command)
        process = command.execute()
        code = process.waitFor()
        assertEquals(0, code)
        out = process.getText()
        assertEquals("OK", out)
    }

    try {
        updateBeConf(ipList[0], httpPortList[0], "disable_auto_compaction", "true");
        updateBeConf(ipList[1], httpPortList[1], "disable_auto_compaction", "true");
        injectionPoint(ipList[0], httpPortList[0], "apply_suite/test_compaction");
        injectionPoint(ipList[1], httpPortList[1], "apply_suite/test_compaction");

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
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """

        sql """ use @regression_cluster_name1; """
        qt_select_default """ SELECT * FROM ${tableName}; """

        sql """ use @regression_cluster_name0; """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        doCompaction.call(ipList[0], httpPortList[0], "cumulative")
        qt_select_default """ SELECT * FROM ${tableName}; """

        sql """ use @regression_cluster_name1; """
        qt_select_default """ SELECT * FROM ${tableName}; """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """

        sql """ use @regression_cluster_name0; """
        qt_select_default """ SELECT * FROM ${tableName}; """

        sql """ use @regression_cluster_name1; """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        sql """ INSERT INTO ${tableName} VALUES (1, "a", 100); """
        doCompaction.call(ipList[1], httpPortList[1], "cumulative")
        doCompaction.call(ipList[1], httpPortList[1], "base")
        qt_select_default """ SELECT * FROM ${tableName}; """

        sql """ use @regression_cluster_name0; """
        qt_select_default """ SELECT * FROM ${tableName}; """
    } finally {
        injectionPoint(ipList[0], httpPortList[0], "clear/all");
        injectionPoint(ipList[1], httpPortList[1], "clear/all");
        updateBeConf(ipList[0], httpPortList[0], "disable_auto_compaction", "false");
        updateBeConf(ipList[1], httpPortList[1], "disable_auto_compaction", "false");
    }
}
