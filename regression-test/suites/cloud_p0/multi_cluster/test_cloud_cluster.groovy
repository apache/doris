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

import groovy.json.JsonOutput
import java.sql.*;

suite("test_cloud_cluster") {
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

    List<List<Object>> result  = sql "show clusters"
    assertTrue(result.size() == 0);

    add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0],
                     "regression_cluster_name0", "regression_cluster_id0");
    add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1],
                     "regression_cluster_name1", "regression_cluster_id1");
    sleep(20000)

    result  = sql "show clusters"
    assertTrue(result.size() == 2);

    sql "use @regression_cluster_name0"
    result  = sql "show clusters"
    for (row : result) {
        println row
        if(row[0] == "regression_cluster_name0") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql "use @regression_cluster_name1"
    result  = sql "show clusters"
    for (row : result) {
        println row
        if(row[0] == "regression_cluster_name1") {
            assertTrue(row[1].toString().toLowerCase() == "true")
        }
    }

    sql "use @regression_cluster_name1"
    sql "create database if not exists test_jdbc_url_db"
    sql "use test_jdbc_url_db"
    sql """
        CREATE TABLE test_table (
            class INT,
            id INT,
            score INT SUM
        )
        AGGREGATE KEY(class, id)
        DISTRIBUTED BY HASH(class) BUCKETS 16
    """

    sql """
        insert into test_table values (666888, 2333333, 10010);
    """

    String jdbcUrl = context.config.jdbcUrl
    String[] urlElems = jdbcUrl.split("/");
    String feEndPoint = urlElems[2]
    String newUrl = "jdbc:mysql://" + feEndPoint + "/" + "test_jdbc_url_db@regression_cluster_name0";
    println("newurl " + newUrl);

    String user = "root";
    String password = "";

    try {
        Connection myCon = DriverManager.getConnection(newUrl, user, password);
        Statement stmt = myCon.createStatement();
        ResultSet rs =  stmt.executeQuery("select * from test_table");
        while (rs.next()) {
            assertEquals(666888, rs.getInt(1));
            assertEquals(2333333, rs.getInt(2));
            assertEquals(10010, rs.getInt(3));
        }
    } catch (SQLException e) {
        System.out.println(e);
        e.printStackTrace()
        assertTrue(false);
    }

    newUrl = "jdbc:mysql://" + feEndPoint + "/" + "@regression_cluster_name0";
    println("newurl " + newUrl);

    try {
        Connection myCon = DriverManager.getConnection(newUrl, user, password);
        Statement stmt = myCon.createStatement();
        ResultSet rs =  stmt.executeQuery("show clusters");
        while (rs.next()) {
            println rs.getString(1)
            if (rs.getString(1) == "regression_cluster_name0") {
                assertEquals(rs.getString(2).toLowerCase(), "true");
            }
        }
    } catch (SQLException e) {
        System.out.println(e);
        e.printStackTrace()
        assertTrue(false);
    }

    newUrl = "jdbc:mysql://" + feEndPoint + "/" + "test_jdbc_url_db";
    println("newurl " + newUrl);

    try {
        Connection myCon = DriverManager.getConnection(newUrl, user, password);
        Statement stmt = myCon.createStatement();
        stmt.execute("use @regression_cluster_name1");
        ResultSet rs =  stmt.executeQuery("select * from test_table");
        while (rs.next()) {
            assertEquals(666888, rs.getInt(1));
            assertEquals(2333333, rs.getInt(2));
            assertEquals(10010, rs.getInt(3));
        }
    } catch (SQLException e) {
        System.out.println(e);
        e.printStackTrace()
        assertTrue(false);
    }

    def executeMySQLCommand = { String command ->
        try {
            String line;
            StringBuilder errMsg = new StringBuilder();
            StringBuilder msg = new StringBuilder();
            Process p = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", command});

            BufferedReader errInput = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            while ((line = errInput.readLine()) != null) {
                errMsg.append(line);
            }
            assert errMsg.length() == 0: "error occurred!" + errMsg.toString();
            errInput.close();

            BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
            while ((line = input.readLine()) != null) {
                msg.append(line);
            }
            println "msg: " + msg.toString()
            assert msg.toString().contains("666888"): "error occurred!" + errMsg.toString();
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String jdbcUrlConfig = context.config.jdbcUrl;
    String tempString = jdbcUrlConfig.substring(jdbcUrlConfig.indexOf("jdbc:mysql://") + 13);
    String mysqlHost = tempString.substring(0, tempString.indexOf(":"));
    String mysqlPort = tempString.substring(tempString.indexOf(":") + 1, tempString.indexOf("/"));
    String cmd1 = "mysql -uroot -h" + mysqlHost + " -P" + mysqlPort + " -Dtest_jdbc_url_db@regression_cluster_name0 " + " -e \" select * from test_table \"";
    executeMySQLCommand(cmd1);
    String cmd2 = "mysql -uroot -h" + mysqlHost + " -P" + mysqlPort + " -D@regression_cluster_name0 " + " -e \" use test_jdbc_url_db; select * from test_table \"";
    executeMySQLCommand(cmd2);
    sql """
        drop table if exists test_table
    """
}
