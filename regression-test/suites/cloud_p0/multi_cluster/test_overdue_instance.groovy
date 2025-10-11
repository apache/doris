import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
*   @Params url is "/xxx", data is request body
*   @Return response body
*/
def http_post(addr, url, data = null, user = null, password = null) {
    // def dst = "http://"+ context.config.feHttpAddress
    def dst = "http://"+ addr
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    logger.info("go into http_post")
    if (user) {
        def authString = user + ":" + password
        def authEncoded = Base64.getEncoder().encodeToString(authString.getBytes())
        conn.setRequestProperty('Authorization', "Basic ${authEncoded}")
    } else {
        conn.setRequestProperty("Authorization", "Basic cm9vdDo=")
    }
    if (data) {
        conn.doOutput = true
        def writer = new OutputStreamWriter(conn.outputStream)
        writer.write(data)
        writer.flush()
        writer.close()
    }
    return conn.content.text
}

class Stmt {
    String stmt
}

class AlterRequest {
    String instance_id;
    String op;
}

class AlterRequestWithoutOp {
    String instance_id
}

suite('test_overdue') {
    def token = context.config.metaServiceToken
    def instance_id = context.config.multiClusterInstance

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

    for (unique_id : beUniqueIdList) {
        resp = get_cluster.call(unique_id);
        for (cluster : resp) {
            if (cluster.type == "COMPUTE") {
                drop_cluster.call(cluster.cluster_name, cluster.cluster_id);
            }
        }
    }
    sleep(20000)

    List<List<Object>> result0  = sql "show clusters"
    assertTrue(result0.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    sleep(20000)

    result0  = sql "show clusters"
    assertTrue(result0.size() == 1);

    for (row : result0) {
        println row
    }

    // add_node.call(beUniqueIdList[1], ipList[1], hbPortList[1],
    //               "regression_cluster_name0", "regression_cluster_id0");

    // sleep(20000)
    sql """ use @regression_cluster_name0 """

    result0  = sql "show clusters"
    assertTrue(result0.size() == 1);

    for (row : result0) {
        println row
    }
    
    def user = 'test_overdue_instance_user'
    def result = sql """ SELECT DATABASE(); """
    def url0 = '/api/query/default_cluster/' + result[0][0]
    def stmt0 = """ show databases """
    def stmt_json = JsonOutput.toJson(new Stmt(stmt: stmt0))


    sql """ drop user if exists ${user} """
    sql """ create user ${user} """
    sql """ GRANT SELECT_PRIV ON *.*.* TO ${user}@'%'; """

    // set wrarehouse to normal
    try {
        try {
            alter_request = JsonOutput.toJson(new AlterRequest(instance_id: instance_id, op: "SET_NORMAL"))
            result = http_post(context.config.metaServiceHttpAddress, "/MetaService/http/set_instance_status?token=greedisgood9999", alter_request)
            obj = new JsonSlurper().parseText(result)
            logger.info("try to set warehouse normal, the result is {}", obj)
            sleep(40000)
        }
        catch (Exception e) {
            logger.info("the error msg is {}", e.getMessage())
        }

        // when warehouse is noraml
        result = sql """ show databases """
        logger.info("when warehouse is normal the result of sql from root is {}", result)

        result = connect(user = "${user}", password = '', url = context.config.jdbcUrl) {
            sql """ show databases """
        }
        logger.info("when warehouse is normal the result of sql from user is {}", result)

        result = http_post(context.config.feHttpAddress, url0, stmt_json)
        def obj = new JsonSlurper().parseText(result)
        logger.info("when warehouse is normal the result of http from root is {}", obj)
        assertEquals(obj.code, 0)

        result = http_post(context.config.feHttpAddress, url0, stmt_json, "${user}", "")
        obj = new JsonSlurper().parseText(result)
        logger.info("when warehouse is normal the result of http from user is {}", obj)
        assertEquals(obj.code, 0)

        // when warehouse is overdue
        try {
            def alter_request = JsonOutput.toJson(new AlterRequest(instance_id: instance_id, op: "SET_OVERDUE"))
            result = http_post(context.config.metaServiceHttpAddress, "/MetaService/http/set_instance_status?token=greedisgood9999", alter_request)
            obj = new JsonSlurper().parseText(result)
            logger.info("try to set warehouse overdue, the result is {}", obj)
            sleep(40000)

            result = sql """ show databases """
            logger.info("when warehouse is overdue the result of sql from root is {}", result)

            try {
                result = connect(user = "${user}", password = '', url = context.config.jdbcUrl) {
                    sql """ show databases """
                }
                logger.info("when warehouse is overdue the result of sql from user is {}", result)
                assertTrue(false, "since warehouse is overdue, the sql from normal user should not success")
            } catch (Exception e) {
                logger.info("when warehouse is overdue the exception of sql from user is {}", e.getMessage())
            }

            result = http_post(context.config.feHttpAddress, url0, stmt_json)
            obj = new JsonSlurper().parseText(result)
            logger.info("when warehouse is overdue the result of http from root is {}", obj)
            assertEquals(obj.code, 0)

            result = http_post(context.config.feHttpAddress, url0, stmt_json, "${user}", "")
            obj = new JsonSlurper().parseText(result)
            logger.info("when warehouse is overdue the result of http from user is {}", obj)
            assertEquals(obj.code, 1)
        } catch (Exception e) {
            alter_request = JsonOutput.toJson(new AlterRequest(instance_id: instance_id, op: "SET_NORMAL"))
            result = http_post(context.config.metaServiceHttpAddress, "/MetaService/http/set_instance_status?token=greedisgood9999", alter_request)
            obj = new JsonSlurper().parseText(result)
            logger.info("try to set warehouse normal, the result is {}", obj)
            throw e
        }

        // when warehouse transforms to normal
        alter_request = JsonOutput.toJson(new AlterRequest(instance_id: instance_id, op: "SET_NORMAL"))
        result = http_post(context.config.metaServiceHttpAddress, "/MetaService/http/set_instance_status?token=greedisgood9999", alter_request)
        obj = new JsonSlurper().parseText(result)
        logger.info("try to set warehouse normal, the result is {}", obj)
        sleep(40000)

        result = sql """ show databases """
        logger.info("when warehouse transforms to normal the result of sql from root is {}", result)

        result = connect(user = "${user}", password = '', url = context.config.jdbcUrl) {
            sql """ show databases """
        }
        logger.info("when warehouse transforms to normal the result of sql from user is {}", result)

        result = http_post(context.config.feHttpAddress, url0, stmt_json)
        obj = new JsonSlurper().parseText(result)
        logger.info("when warehouse transforms to normal the result of http from root is {}", obj)
        assertEquals(obj.code, 0)

        result = http_post(context.config.feHttpAddress, url0, stmt_json, "${user}", "")
        obj = new JsonSlurper().parseText(result)
        logger.info("when warehouse transforms to normal the result of http from user is {}", obj)
        assertEquals(obj.code, 0)

        // test the case when op parametr is not set
        try {
            alter_request = JsonOutput.toJson(new AlterRequestWithoutOp(instance_id: instance_id))
            result = http_post(context.config.metaServiceHttpAddress, "/MetaService/http/set_instance_status?token=greedisgood9999", alter_request)
            obj = new JsonSlurper().parseText(result) 
            logger.info("the result is {}", obj)
            assertTrue(false, "it's not a vlid request")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("400"))
        }
    } finally {
        try_sql("DROP USER ${user}")
    }
}
