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

import java.time.LocalDateTime
import java.time.Duration
import java.time.format.DateTimeFormatter



suite('test_manager_interface_4',"p0") {


// show proc '/current_query_stmts'
// show proc '/current_queries'
// show processlist
// kill query $query_id
// SHOW PROC '/cluster_health/tablet_health' 
    def test_proc = {

        def futures = []
        

        sql """set parallel_pipeline_task_num=1;"""
        futures.add( thread {

            try{
                sql """ select *  from numbers("number" = "9910") as  a  join numbers('number'="18880094567") as b on a.number = b.number; """
            }catch(Exception e){
                
            }
        })

        futures.add( thread {
            sleep(500);
            List<List<Object>> result = sql """ show proc '/current_query_stmts' """ 
            logger.info("result = ${result}" )
            def x = 0
            def queryid = ""
            logger.info("result = ${result}")

            for( int i = 0;i<result.size();i++) {
                if (result[i][7] != null && result[i][7].contains("18880094567") )//Statement
                {
                    x = 1
                    queryid = result[i][0]
                    logger.info("query ID = ${queryid}")
                    assertTrue(result[i][0]!= null) //QueryId
                    assertTrue(result[i][1]!= null) //ConnectionId
                    assertTrue(result[i][2]!= null)//Catalog
                    assertTrue(result[i][3]!= null)//Database
                    assertTrue(result[i][4]!= null)//User
                    assertTrue(result[i][5]!= null)//ExecTime
                    assertTrue(result[i][5].isNumber())//ExecTime
                    assertTrue(result[i][6]!= null)//SqlHash
                }
            }
            assertTrue(x == 1)
            
            x = 0 
            result = sql """  show proc '/current_queries' """
            logger.info("result = ${result}")
            for( int i = 0;i<result.size();i++) {
                if (result[i][0] == queryid )//QueryId
                {
                    x = 1
                    assertTrue(result[i][5]!= null)//ScanBytes
                    assertTrue(result[i][6]!= null)//ProcessBytes
                }
            }
            assertTrue(x == 1)

            result = sql """  show processlist  """
            logger.info("result = ${result}")
            for( int i =0 ;i < result.size();i++ ){
                assertTrue( result[i][2].toLowerCase() != "null"  )//User
                assertTrue( result[i][3].toLowerCase() != "null"  )//Host
                assertTrue( result[i][5].toLowerCase() != "null"  )//Catalog
                assertTrue( result[i][6].toLowerCase() != "null"  )//Db
                assertTrue( result[i][10].toLowerCase() != "null"  )//QueryId
                if (result[i][10] == queryid) {
                    x = 1
                }
            }

            assertTrue(x == 1)
            sql """ kill query "${queryid}" """
            
            x =  0
            sleep(3000)

            result = sql """  show proc '/current_queries' """
            logger.info("result = ${result}")
            for( int i = 0;i<result.size();i++) {
                if (result[i][0] == queryid )//QueryId
                {
                    x = 1
                }
            }
            assertTrue(x == 0)
        })
        futures.each { it.get() }


        def tablet_num  = 0;
        def healthy_num = 0;
        def total_tablet_num  = 0;
        def total_healthy_num = 0;
        def result = sql """  SHOW PROC '/cluster_health/tablet_health' """ 
        logger.info("result = ${result}" )

        for( int i =0 ;i < result.size();i++ ){
            assertTrue(result[i][0].toLowerCase() != null ) // DbId
            if (result[i][0].toLowerCase() == "total") {
                total_tablet_num = result[i][2].toBigInteger();
                total_healthy_num = result[i][3].toBigInteger();
            }else {
                tablet_num += result[i][2].toBigInteger();
                healthy_num += result[i][3].toBigInteger();
                
            }
            // assertTrue(result[i][2]()) // TabletNum
            // assertTrue(result[i][3]()) // HealthyNum
        }
        assertTrue(total_healthy_num == healthy_num )
        assertTrue(total_healthy_num == healthy_num )

    }
    test_proc();
}