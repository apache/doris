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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType




suite('test_manager_interface_2',"p0") {

// show data   && show data from 'table_name'"
    def test_table_data = {
        sql """ drop database if exists  test_manager_tb_case_5 """
        sql """create database test_manager_tb_case_5 """
        sql """ use test_manager_tb_case_5 """ 

        sql """ 
        create table test_manager_tb
        (
            k1 TINYINT,
            k2 DECIMAL(10, 2) DEFAULT "10.05",    
            k3 CHAR(10) COMMENT "string column",    
            k4 INT NOT NULL DEFAULT "1" COMMENT "int column",
            k5 STRING 
        ) COMMENT "manager_test_table"
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1',
        "bloom_filter_columns" = "k3,k5"
        );"""

    
        result =sql  """ show data """ 
        for(int i = 0 ; i < result.size();i++) {
            assert(result[i][0].toLowerCase() != "null") //TableName 
            assert(result[i][1].toLowerCase() != "null") //Size 
            assert(result[i][2].isNumber()) //ReplicaCount 
        }   
        
        result =sql """ show data from  test_manager_tb """ 
        for(int i = 0 ; i < result.size();i++) {
            if (i +1 != result.size()) {
                assert(result[i][0].toLowerCase() != "null") //TableName 
            }

            assert(result[i][2].toLowerCase() != "null") //Size 
            assert(result[i][3].isNumber()) //ReplicaCount 
        }

        sql """ drop database if exists  test_manager_tb_case_5 """
    }
    test_table_data()


// show partitions from '$table'
    def test_partitions = {

        sql """ drop database if exists  test_manager_partitions_case """
        sql """create database test_manager_partitions_case """
        sql """ use test_manager_partitions_case """ 
    
        sql """ create table test_manager_tb
        (
            k1 TINYINT,
            k2 CHAR(10) COMMENT "string column",    
            k3 INT NOT NULL DEFAULT "1" COMMENT "partition column",
        ) COMMENT "manager_test_load_table"
        PARTITION by Range(k3)
        (
            PARTITION `less100` VALUES LESS THAN (100),
            PARTITION `less200` VALUES LESS THAN (200),
            PARTITION `less2000` VALUES LESS THAN (2000)
        )
        DISTRIBUTED BY HASH(k1) BUCKETS 1 
        PROPERTIES ('replication_num' = '1');
        """
        
        List<List<Object>> result = sql """ show partitions from  test_manager_tb """ 
        logger.info("result = ${result}" ) 
        for( int i =0 ; i <result.size();i++) {
            assertTrue( result[i][1] in ["less100","less200","less2000"])

            logger.info (result[i][1] + ".ReplicationNum = " + result[i][9])
            
            assertTrue( result[i][9].toBigInteger() == 1  ||result[i][9].toBigInteger() == 3 )  // ReplicationNum
        }


        sql """ drop database if exists  test_manager_partitions_case """

    }
    test_partitions()



// show load from "$db" where state='$status'
    def test_load = {

        sql """ drop database if exists  test_manager_load_case """
        sql """create database test_manager_load_case """
        sql """ use test_manager_load_case """ 
    
        sql """ create table test_manager_tb
        (
            k1 TINYINT,
            k2 CHAR(10) COMMENT "string column",    
            k3 INT NOT NULL DEFAULT "1" COMMENT "int column",
        ) COMMENT "manager_test_load_table"
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1');"""

        List<List<Object>> result = sql """ insert into test_manager_tb values (1,"hell0",10);""" 
        assertTrue(result[0][0] == 1)
        result = sql """insert into test_manager_tb values (2,"hell0",20); """ 
        assertTrue(result[0][0] == 1)
        result = sql """insert into test_manager_tb values (3,"hell0",30);"""
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (4,"hell0",40);""" 
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        assertTrue(result[0][0] == 1)


            
        sql """ create table test_manager_tb_2
        (
            k1 TINYINT,
            k2 CHAR(10) COMMENT "string column"
        ) COMMENT "manager_test_load_table_2"
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1');"""

        result = sql """ insert into test_manager_tb_2 values (1,"hell0");""" 
        assertTrue(result[0][0] == 1)
        result = sql """insert into test_manager_tb_2 values (2,"hell0"); """ 
        assertTrue(result[0][0] == 1)

        result = sql """  show load from test_manager_load_case where state='FINISHED'  """
        for(int i =0 ;i< result.size();i++) {
            assertTrue(result[i][0].toLowerCase() != "null"  )//JobId 
            assertTrue(result[i][0].toBigInteger() != 0  )//JobId 
            assertTrue(result[i][2]  == "FINISHED") //State 
            assertTrue(result[i][3].contains( result[i][0]))// Progress 
        }




        sql """ drop database if exists  test_manager_load_case """
    }
    test_load();


// show backends
//alter system modify backend 
// show frontends
// show broker
// ALTER TABLE internal.__internal_schema.column_statistics SET ("replication_num" = "1")
    def test_system = {


        def address = "127.0.0.1"
        def notExistPort = 12346
        def notExistPort2 = 234567
        try {
            sql """ALTER SYSTEM DROPP BACKEND  "${address}:${notExistPort}";"""
        }catch (Exception e) {
        }
    
        sql """ALTER SYSTEM ADD BACKEND "${address}:${notExistPort}";"""

        result = sql """SHOW BACKENDS;"""
        logger.info("result = ${result}" )
        def x = 0 
        for(int i  =0 ;i<result.size();i++) {
            //HeartbeatPort: 
            
            if (result[i][2].toBigInteger() == notExistPort) {
                assertTrue(result[i][0]!=null)//name 

                assert(result[i][19].toLowerCase() != "null") // TAG 
                //Tag: {"location" : "default"}
                def json = parseJson(result[i][19])
                assertEquals("default", json.location)
                
                assertEquals(result[i][3].toBigInteger(),-1)//BePort: -1
                assertEquals(result[i][4].toBigInteger(),-1)//HttpPort: -1
                assertEquals(result[i][5].toBigInteger(),-1)// BrpcPort: -1
                assertEquals(result[i][6].toBigInteger(),-1)//ArrowFlightSqlPort: -1
                assertEquals(result[i][7],null)//LastStartTime : NULL 
                assertEquals(result[i][8],null)//LastHeartbeat: NULL
                assertEquals(result[i][9],"false")// Alive: false
                assertEquals(result[i][10],"false")//SystemDecommissioned: false
                assertEquals(result[i][11].toBigInteger(),0)// TabletNum: 0
                assertEquals(result[i][12],"0.000 ")//  DataUsedCapacity: 0.000 
                assertEquals(result[i][13],"0.000 ")//   TrashUsedCapacity: 0.000 
                assertEquals(result[i][14],"1.000 B")//       AvailCapacity: 1.000 B
                assertEquals(result[i][15],"0.000 ")    //       TotalCapacity: 0.000 
                assertEquals(result[i][16],"0.00 %")    //             UsedPct: 0.00 %
                assertEquals(result[i][17],"0.00 %")    //      MaxDiskUsedPct: 0.00 %

                assertTrue( result[i][22].contains("lastSuccessReportTabletsTime") )//Status 
                assertTrue( result[i][22].contains("""isQueryDisabled":false""") )
                assertTrue( result[i][22].contains("""isLoadDisabled":false""") )
                x++ 
        
            }
        }
        assertTrue(x==1)
        logger.info("result:${result}")

        sql """ALTER SYSTEM MODIFY BACKEND "${address}:${notExistPort}" SET ("disable_query" = "true"); """
        sql """ALTER SYSTEM MODIFY BACKEND "${address}:${notExistPort}" SET ("disable_load" = "true"); """
        result = sql """SHOW BACKENDS;"""
        logger.info("result = ${result}" )
        x = 0
        for(int i  =0 ;i<result.size();i++) {
            //HeartbeatPort: 
            if (result[i][2].toBigInteger() == notExistPort) {
                x ++ 
                assertTrue( result[i][22].contains("lastSuccessReportTabletsTime") )//Status 
                assertTrue( result[i][22].contains("""isQueryDisabled":true""") )
                assertTrue( result[i][22].contains("""isLoadDisabled":true""") )
            }
        }
        assertTrue(x==1)
        logger.info("result:${result}")
        sql """ALTER SYSTEM DROPP BACKEND "${address}:${notExistPort}";"""

        result = sql """SHOW BACKENDS;"""
        logger.info("result:${result}")
        x = 0 
        for(int i  =0 ;i<result.size();i++) {
            //HeartbeatPort: 
            if (result[i][2].toBigInteger() == notExistPort) {
                x ++ 
            }
        }        
        assertTrue(x==0)


        result = sql """ SHOW FRONTENDS """
        logger.info("result = ${result}" )
        x = 0
        for(int i  =0 ;i<result.size();i++) {
            if (result[i][18]=="Yes") {
                assertTrue(result[i][0]!=null) // name 
                assertTrue(result[i][1]!=null) // Host 
                assertTrue(result[i][2]!=null) // EditLogPort
                assertTrue(result[i][2].isNumber()) // EditLogPort
                assertTrue(result[i][3]!=null) // HttpPort 
                assertTrue(result[i][3].isNumber()) // HttpPort 
                assertTrue(result[i][4]!=null) // QueryPort 
                assertTrue(result[i][4].isNumber()) // QueryPort 

                assertTrue(result[i][5]!=null) // RpcPort
                assertTrue(result[i][5].isNumber()) // RpcPort
                assertTrue(result[i][6]!=null) // ArrowFlightSqlPort 
                assertTrue(result[i][6].isNumber()) // ArrowFlightSqlPort
                assertTrue(result[i][7] in ["FOLLOWER","OBSERVER"]) // role 
                assertTrue(result[i][8] in ["true","false"]) // isMaster 
                assertTrue(result[i][9]!=null) // ClusterId
                assertTrue(result[i][9].isNumber()) // ClusterId
                assertTrue(result[i][10]!=null) // Join
                assertTrue(result[i][10] in ["true","false"]) 
                assertTrue(result[i][11]!=null) //Alive
                assertTrue(result[i][11] in ["true","false"]) //Alive
                assertTrue(result[i][17]!=null) //Version 
                x ++
            }
        }
        assertTrue(x==1)

        try{
            sql """ALTER SYSTEM DROP BROKER test_manager_broker "${address}:${notExistPort}";"""
        }catch(Exception e){
            
        }

        sql """ALTER SYSTEM ADD BROKER test_manager_broker "${address}:${notExistPort}";"""
        result = sql """ show broker """ 
        x =  0
        logger.info("result = ${result}" )
        for ( int i =0 ;i<result.size();i++){

            assertTrue(result[i][3] in ["true","false"])//Alive


            if (result[i][0]=="test_manager_broker"){
                x ++
                assertEquals(result[i][1].toString(),address) //Host 
                assertEquals(result[i][2].toString(),notExistPort.toString()) //Port 
                assertTrue(result[i][3] =="false")//Alive

                assertTrue(result[i][4]==null)//LastStartTime:NULL
                assertTrue(result[i][5]==null)//LastUpdateTime:NULL
            }
        }
        assertTrue(x==1)
        sql """ALTER SYSTEM DROP BROKER test_manager_broker "${address}:${notExistPort}";"""



        sql """ ALTER TABLE internal.__internal_schema.column_statistics SET ("replication_num" = "1") """

    }
    test_system()


}
