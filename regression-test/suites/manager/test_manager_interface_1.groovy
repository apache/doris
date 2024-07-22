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



suite('test_manager_interface_1',"p0") {



    logger.info("test_manager_interface_1 start")

    sql """ switch internal """


    String jdbcUrl = context.config.jdbcUrl
    def tokens = context.config.jdbcUrl.split('/')
    jdbcUrl=tokens[0] + "//" + tokens[2] + "/" + "?"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"



//select * from internal.information_schema.schemata
    def test_schemata = {
        logger.info("TEST select * from internal.information_schema.schemata");

        List<List<Object>>  schemata = sql   """select * from internal.information_schema.schemata"""
        for (int i = 0; i < schemata.size(); i++) {
            assertTrue(!schemata[i][0].isEmpty()) // CATALOG_NAME
            assertTrue(schemata[i][0].toLowerCase() != "null") 

            assertTrue(!schemata[i][1].isEmpty()) // SCHEMA_NAME
            assertTrue(schemata[i][1].toLowerCase() != "null") 
        }
        List<List<Object>>  schemata2 = sql   """select * from internal.information_schema.schemata where CATALOG_NAME = "internal" and  SCHEMA_NAME = "__internal_schema" """
        assertTrue(schemata2.size() == 1)
        
        sql """ drop database if exists internal.test_information_schemata_1; """
        sql """ create database internal.test_information_schemata_1; """
        List<List<Object>>  schemata3 = sql   """select * from internal.information_schema.schemata where CATALOG_NAME = "internal" and  SCHEMA_NAME = "test_information_schemata_1" """
        assertTrue(schemata3.size() == 1)

        sql """ drop database internal.test_information_schemata_1; """
        List<List<Object>>  schemata4 = sql   """select * from internal.information_schema.schemata where CATALOG_NAME = "internal" and  SCHEMA_NAME = "test_information_schemata_1" """
        assertTrue(schemata4.size() == 0)
    }
    test_schemata()


//select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from information_schema.tables
    def test_information_tables = {
        logger.info("TEST select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from information_schema.tables")

        List<List<Object>>  result = sql   """select TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE from information_schema.tables"""
        for(int i = 0;i<result.size();i++) {
            assertTrue(!result[i][0].isEmpty())
            assertTrue(!result[i][1].isEmpty())
            assertTrue(!result[i][2].isEmpty())
            assertTrue(!result[i][3].isEmpty())
            assertTrue(result[i][0].toLowerCase() != "null")
            assertTrue(result[i][1].toLowerCase() != "null")
            assertTrue(result[i][2].toLowerCase() != "null")
            assertTrue(result[i][3].toLowerCase() != "null")

            assertTrue( result[i][3] in ["SYSTEM VIEW","VIEW","BASE TABLE"] )
        }

    } 
    test_information_tables()

//select * from information_schema.metadata_name_ids
    def test_metadata_name_ids = {
        logger.info("TEST select * from information_schema.metadata_name_ids")
        List<List<Object>>  result = sql   """select * from information_schema.metadata_name_ids """
        def tableName = "internal.information_schema.metadata_name_ids"
        sql """ create database if not exists test_manager_metadata_name_ids; """
        sql """ use test_manager_metadata_name_ids ; """ 

    	qt_metadata_1 """ select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${tableName}
	    	where CATALOG_NAME="internal" and DATABASE_NAME ="test_manager_metadata_name_ids"  """ 

        sql """ create table if not exists test_metadata_name_ids  (
            a int ,
            b varchar(30)
        )
        DUPLICATE KEY(`a`)
        DISTRIBUTED BY HASH(`a`) BUCKETS 10
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        ); """

    	qt_metadata_2 """ select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${tableName}
	    	where CATALOG_NAME="internal" and DATABASE_NAME ="test_manager_metadata_name_ids"  """ 
    
	    sql """ drop table test_metadata_name_ids """ 

        qt_metadata_2 """ select CATALOG_NAME,DATABASE_NAME,TABLE_NAME from ${tableName}
            where CATALOG_NAME="internal" and DATABASE_NAME ="test_manager_metadata_name_ids" and TABLE_NAME="test_metadata_name_ids";""" 
    }
    test_metadata_name_ids()



//show catalogs
//alter catalog $old_name rename $new_name
//drop catalog $catalog_name
    def test_catalogs = {
        logger.info("TEST show/rename/drop catalogs")
        def  catalog_name = "test_manager_catalogs_case"
        // println jdbcUrl
        sql """ drop catalog if exists ${catalog_name}"""
        sql  """ CREATE CATALOG `${catalog_name}` PROPERTIES(
                "user" = "${jdbcUser}",
                "type" = "jdbc",
                "password" = "${jdbcPassword}",
                "jdbc_url" = "${jdbcUrl}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""

        List<List<Object>>  result = sql   """ show catalogs """
        //CatalogName
        def x = 0
        for( int i  =0 ;i < result.size();i++ ) {
            assertTrue(result[i][1].toLowerCase() != "null")
            if ( result[i][1].toLowerCase() == catalog_name) {
                x = 1
            }
        }
        assertTrue(x == 1) 
        
        x = 0 
        sql """ alter catalog ${catalog_name} rename ${catalog_name}_rename """
        result = sql   """ show catalogs """
        for( int i  =0 ;i < result.size();i++ ) {
            assertTrue(result[i][1].toLowerCase() != "null")
            if ( result[i][1].toLowerCase() == catalog_name + "_rename") {
                x = 1
            }
        }
        assertTrue(x == 1) 
        
        x = 0 
        sql """ drop catalog ${catalog_name}_rename""" 
        result = sql   """ show catalogs """
        for( int i  =0 ;i < result.size();i++ ) {
            assertTrue(result[i][1].toLowerCase() != "null")
            if ( result[i][1].toLowerCase() == catalog_name + "_rename") {
                x = 1
            }
        }
        assertTrue(x == 0) 
    }
    test_catalogs()



//show databases
//alter database $old_name rename $new_name
//create database $database_name
    def test_databases = {
        logger.info("TEST show/rename/drop databases")
        def  databases_name  = "test_manager_db_case"


        sql """ switch  internal """ 
        sql """ drop database if exists ${databases_name} """ 
        sql """ drop database if exists ${databases_name}_rename """ 

        sql """ create database ${databases_name} """ 
        List<List<Object>>  result = sql   """ show databases """
    
        def x = 0        
        for( int i  =0 ;i < result.size();i++ ) {
            assert(result[i].size() == 1)
            assertTrue(result[i][0].toLowerCase() != "null") //Database
            if ( result[i][0].toLowerCase() == databases_name) {
                x = 1
            }
        }
        assertTrue(x == 1) 
        
        x = 0 
        sql """ alter database ${databases_name} rename ${databases_name}_rename """
        result = sql   """ show databases """
        for( int i  =0 ;i < result.size();i++ ) {
            assertTrue(result[i][0].toLowerCase() != "null")
            if ( result[i][0].toLowerCase() == databases_name + "_rename") {
                x = 1
            }
        }
        assertTrue(x == 1) 
        
        x = 0 
        sql """ drop database ${databases_name}_rename""" 
        result = sql   """ show databases """
        for( int i  =0 ;i < result.size();i++ ) {
            assertTrue(result[i][0].toLowerCase() != "null")
            if ( result[i][0].toLowerCase() == databases_name + "_rename") {
                x = 1
            }
        }
        assertTrue(x == 0) 
    }
    test_databases()




// show tables  && show tables like '$table_name'
//desc $table_name && desc $table_name all
//show create table '$table_name'
//drop tables
    def test_show_tables = {
        sql """ drop database if exists  test_manager_tb_case """
        sql """create database test_manager_tb_case"""
        sql """ use test_manager_tb_case """ 
        
        List<List<Object>>  result = sql """ show tables """ 
        assertTrue(result.size() == 0)        
        result = sql """ show tables like 'test_manager_tb%' """ 
        assertTrue(result.size() == 0)

        sql """ 
        create table test_manager_tb_1
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

        qt_tables_1 """ desc test_manager_tb_1 """ 
        qt_tables_2 """ desc test_manager_tb_1 all""" 


        result = sql """ show tables """ 
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)     
        assertTrue(result[0][0].toLowerCase() ==  "test_manager_tb_1")     
        result = sql """ show tables like 'test_manager_tb%' """ 
        assertTrue(result.size() == 1)
        assertTrue(result[0].size() == 1)     
        assertTrue(result[0][0].toLowerCase() ==  "test_manager_tb_1")

        
        result = sql """ show create table  test_manager_tb_1"""   
        assertTrue(result[0][0] == "test_manager_tb_1") // TABLE NAME
        // assertTrue(result[0][1].substring() == "test_manager_tb_1") //DDL 
        def ddl_str =  result[0][1] 
        def idx =  ddl_str.indexOf("PROPERTIES")
        assertTrue(idx != -1 );
        assertTrue( ddl_str.startsWith("""CREATE TABLE `test_manager_tb_1` (
  `k1` TINYINT NULL,
  `k2` DECIMAL(10, 2) NULL DEFAULT "10.05",
  `k3` CHAR(10) NULL COMMENT 'string column',
  `k4` INT NOT NULL DEFAULT "1" COMMENT 'int column',
  `k5` TEXT NULL
) ENGINE=OLAP
DUPLICATE KEY(`k1`, `k2`, `k3`)
COMMENT 'manager_test_table'
DISTRIBUTED BY HASH(`k1`) BUCKETS 1"""))

        sql """ drop table test_manager_tb_1 """ 
        result = sql """ show tables """ 
        assertTrue(result.size() == 0)        
        result = sql """ show tables like 'test_manager_tb%' """ 
        assertTrue(result.size() == 0)

        sql """ drop database test_manager_tb_case """ 
    }
    test_show_tables()


//alter table $table_name set ("$key" = "$value")
    def test_tables_PROPERTIES  = {
        sql """ drop database if exists  test_manager_tb_properties_case FORCE"""
        sql """create database test_manager_tb_properties_case"""
        sql """ use test_manager_tb_properties_case """ 
        

        sql """ create table test_manager_tb_2
        (
            k1 TINYINT,
            k2 DECIMAL(10, 2) DEFAULT "10.05",    
            k3 CHAR(10) COMMENT "string column",    
            k4 INT NOT NULL DEFAULT "1" COMMENT "int column",
            k5 STRING 
        ) COMMENT "manager_test_table"
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1',
        "bloom_filter_columns" = "k3"
        );"""
    
        List<List<Object>> result = sql """ show  create table  test_manager_tb_2 """ 
        logger.info("result = ${result}" ) 
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "test_manager_tb_2")
        if (!isCloudMode()) {
            def ddl_str =  result[0][1] 
            def idx =  ddl_str.indexOf("min_load_replica_num")
            assertTrue( ddl_str.substring(idx,ddl_str.length()).startsWith("""min_load_replica_num" = "-1"""))

            sql """alter table test_manager_tb_2 set ("min_load_replica_num" = "1")"""
            result = sql """ show  create table  test_manager_tb_2 """ 
            assertTrue(result[0][0] == "test_manager_tb_2")
            ddl_str =  result[0][1] 
            idx =  ddl_str.indexOf("min_load_replica_num")
            assertTrue( ddl_str.substring(idx,ddl_str.length()).startsWith("""min_load_replica_num" = "1"""))
        }

        sql """ DROP table  test_manager_tb_2 FORCE"""
        sql """ drop database test_manager_tb_properties_case FORCE""" 

    }
    test_tables_PROPERTIES()


// show table status from $db_name like '$table_name'
    def test_tables_status = {
        
        sql """ drop database if exists  test_manager_tb_case_3 """
        sql """create database test_manager_tb_case_3 """
        sql """ use test_manager_tb_case_3 """ 
        
        def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        def now = LocalDateTime.now()
        def formattedNow = now.format(formatter)
        logger.info( " NOW TIME =  ${formattedNow} " )

        sql """ create table test_manager_tb
        (
            k1 TINYINT,
            k2 CHAR(10) COMMENT "string column",    
            k3 INT NOT NULL DEFAULT "1" COMMENT "int column",
        ) COMMENT "manager_test_table"
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1');"""


        List<List<Object>>  result = sql  """ show table status from test_manager_tb_case_3 like 'test_manager_tb%' """ 
        logger.info("result = ${result}" ) 
        assertTrue(result[0][4] == 0 )// Rows

        def create_time = result[0][11] //Create_time
        def duration = Duration.between(now, create_time)
        assertTrue(Math.abs(duration.toHours()) < 2)
        logger.info( "table test_manager_tb  Create TIME =  ${create_time} " )
        
        def update_time1 = result[0][12]//Update_time
        duration = Duration.between(now, update_time1)
        assertTrue(Math.abs(duration.toHours()) < 2)
        logger.info( "table test_manager_tb  Update TIME =  ${update_time1} " )

        assertTrue(  "manager_test_table" == result[0][17] ) //Comment

        result = sql """ insert into test_manager_tb values (1,"hell0",10);""" 
        assertTrue(result[0][0] == 1)
        result = sql """insert into test_manager_tb values (2,"hell0",20); """ 
        assertTrue(result[0][0] == 1)
        result = sql """insert into test_manager_tb values (3,"hell0",30);"""
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (4,"hell0",40);""" 
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        assertTrue(result[0][0] == 1)

        def j = 0 ;
        def retryTime = 100;
        for (j  =0 ;j < retryTime;j++) {
            sql """ select  * from test_manager_tb_case_3.test_manager_tb; """ 
            result =  sql """ show table status from test_manager_tb_case_3 like 'test_manager_tb%' """ 
            if ( result[0][4] == 5) {
                assert( create_time == result[0][11])//Create_time
                assertTrue(  "manager_test_table" == result[0][17] ) //Comment
                def update_time2 = result[0][12] //Update_time
                duration = Duration.between(now, update_time2)
                assertTrue(Math.abs(duration.toHours()) < 5)
                logger.info( "table test_manager_tb  Update TIME =  ${update_time2} " )
                
                break;
            } 
            sleep(10000)
        }
        if (j == retryTime) {
            logger.info("result = ${result}" )
            logger.info("  TEST show table status from $db_name like '$table_name';ROWS UPDATE FAIL.");
            assertTrue(false);
        }



        sql """ drop table test_manager_tb """
        sql """ drop database if exists  test_manager_tb_case_3 """
    }
    test_tables_status()
        


//show index from '$table_name'
    def test_table_index = {
        sql """ drop database if exists  test_manager_tb_case_4 """
        sql """create database test_manager_tb_case_4 """
        sql """ use test_manager_tb_case_4 """ 

        sql """ 
        create table test_manager_tb
        (
            k1 TINYINT,
            k2 CHAR(10) COMMENT "string column",    
            k3 INT NOT NULL DEFAULT "1" COMMENT "int column",
        ) COMMENT "manager_test_table"
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES ('replication_num' = '1',
        "bloom_filter_columns" = "k2");"""

        

        List<List<Object>>   result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        logger.info("result = ${result}" )
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        assertTrue(result[0][0] == 1)
        result = sql """ insert into test_manager_tb values (5,"hell0",50); """ 
        assertTrue(result[0][0] == 1)

        sql """  CREATE INDEX  bitmap_index_name ON test_manager_tb (k1) USING BITMAP COMMENT 'bitmap_k1'; """ 

        def j = 0 ;
        def retryTime = 100;
        for (j  =0 ;j < retryTime;j++) {
            result = sql """ show index from test_manager_tb; """ 
            
            if (result.size() == 1){
                break;
            }
            sleep(1000);
        }        
        logger.info("result = ${result}" )

        if (j == retryTime) {
            logger.info("  TEST show index from '$table_name' FAIL.");
            assertTrue(false);
        }


        
        assertTrue(result[0][2] == "bitmap_index_name" )//Key_name
        assertTrue(result[0][4] == "k1" )//Column_name
        assertTrue(result[0][10] == "BITMAP" || result[0][10] == "INVERTED" ) //BITMAP
        assertTrue(result[0][11] == "bitmap_k1" ) //bitmap_siteid

        sql """ drop INDEX bitmap_index_name on test_manager_tb;""" 

    

    
        sql """ drop table test_manager_tb FORCE """
        sql """ drop database if exists  test_manager_tb_case_4  FORCE"""

    }
    test_table_index()





// show proc '/current_query_stmts'
// show proc '/current_queries'
// show processlist
// kill query $query_id
// SHOW PROC '/cluster_health/tablet_health' 
    def test_proc = {

        def futures = []
        

        futures.add( thread {

            try{
                sql """ select sleep(4.809); """
            }catch(Exception e){
                
            }
        })
        futures.add( thread {
            sleep(1000);
            List<List<Object>> result = sql """ show proc '/current_query_stmts' """ 
            logger.info("result = ${result}" )
            def x = 0
            def queryid = ""
            logger.info("result = ${result}")

            for( int i = 0;i<result.size();i++) {
                if (result[i][7] != null && result[i][7].contains("sleep(4.809)") )//Statement
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
            sleep(5000)

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
        result = sql """  SHOW PROC '/cluster_health/tablet_health' """ 
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



//select a.*, b.*, c.NAME as WORKLOAD_GROUP_NAME from information_schema.active_queries a left join information_schema.backend_active_tasks b on a.QUERY_ID = b.QUERY_ID left join information_schema.workload_groups c on a.WORKLOAD_GROUP_ID = c.ID
    def  test_active_query = {

        List<List<Object>> result = sql """ select 1;"""


        def futures = []
        futures.add( thread {
            
            try{
                sql """ select sleep(4.7676); """
            }catch(Exception e){
            }
        })

        futures.add( thread {
            sleep(1500)

            result = sql """ 
            select a.*, b.*, c.NAME as WORKLOAD_GROUP_NAME from information_schema.active_queries a left join 
            information_schema.backend_active_tasks b on a.QUERY_ID = b.QUERY_ID left join information_schema.workload_groups c on a.WORKLOAD_GROUP_ID = c.ID
            """
            logger.info("result = ${result}")
            
            def x = 0
            def queryId = ""
            for( int i =0 ;i < result.size();i++ ){
                assertTrue(result[i][0] != null ) // QueryId

                if ( result[i][9].contains("sleep(4.7676)")  ){
                    x = 1 
                    queryId = result[i][0]
                    logger.info("result = ${queryId}}")

                    assertTrue(result[i][2]!=null) // QUERY_TIME_MS  
                    assertTrue(result[i][14]!=null) // TASK_CPU_TIME_MS   
                    assertTrue(result[i][15].toBigInteger() ==0 ) // SCAN_ROWS  
                    assertTrue(result[i][16].toBigInteger() ==0)//SCAN_BYTES
                    assertTrue(result[i][19].toBigInteger() ==0) // SHUFFLE_SEND_BYTES     
                    assertTrue(result[i][20].toBigInteger() ==0) // SHUFFLE_SEND_ROWS   
                    assertTrue(result[i][18]!=null) // CURRENT_USED_MEMORY_BYTES   
                    assertTrue(result[i][22]!=null) // WORKLOAD_GROUP_NAME              
                }
            }
            assertTrue(x == 1)
            sql """ kill query "${queryId}" """ 
        })
        futures.each { it.get() }
    }
    test_active_query()



//select * from __internal_schema.audit_log 
    def test_audit_log = {
    
        sql """ set global enable_audit_plugin = true; """ 
        List<List<Object>> result =sql  """ show create table __internal_schema.audit_log; """ 
        logger.info("result = ${result}" )
        
        assertTrue(result[0][0] == "audit_log")

        assertTrue(result[0][1].contains("CREATE TABLE `audit_log`"))
        assertTrue(result[0][1].contains("`query_id` VARCHAR(48) NULL,"))
        assertTrue(result[0][1].contains("`time` DATETIME(3) NULL,"))
        assertTrue(result[0][1].contains("`client_ip` VARCHAR(128) NULL,")) 
        assertTrue(result[0][1].contains("`user` VARCHAR(128) NULL,")) 
        assertTrue(result[0][1].contains("`catalog` VARCHAR(128) NULL")) 
        assertTrue(result[0][1].contains("`db` VARCHAR(128) NULL,"))
        assertTrue(result[0][1].contains("`state` VARCHAR(128) NULL"))
        assertTrue(result[0][1].contains("`error_code` INT NULL,"))
        assertTrue(result[0][1].contains("`error_message` TEXT NULL,"))
        assertTrue(result[0][1].contains("`query_time` BIGINT NULL,"))
        assertTrue(result[0][1].contains("`scan_bytes` BIGINT NULL,"))
        assertTrue(result[0][1].contains("`scan_rows` BIGINT NULL,"))
        assertTrue(result[0][1].contains("`return_rows` BIGINT NULL,")) 
        assertTrue(result[0][1].contains("`stmt_id` BIGINT NULL,"))
        assertTrue(result[0][1].contains("`is_query` TINYINT NULL,"))
        assertTrue(result[0][1].contains("`frontend_ip` VARCHAR(128) NULL,"))
        assertTrue(result[0][1].contains("`cpu_time_ms` BIGINT NULL,"))
        assertTrue(result[0][1].contains("`sql_hash` VARCHAR(128) NULL,"))
        assertTrue(result[0][1].contains("`sql_digest` VARCHAR(128) NULL,"))
        assertTrue(result[0][1].contains("`peak_memory_bytes` BIGINT NULL,"))
        assertTrue(result[0][1].contains("`workload_group` TEXT NULL,"))
        assertTrue(result[0][1].contains("`stmt` TEXT NULL"))

        assertTrue(result[0][1].contains("ENGINE=OLAP"))

        assertTrue(result[0][1].contains("DUPLICATE KEY(`query_id`, `time`, `client_ip`)"))
        assertTrue(result[0][1].contains("""PARTITION BY RANGE(`time`)"""))
        assertTrue(result[0][1].contains("""dynamic_partition.enable" = "true"""))
        assertTrue(result[0][1].contains("""dynamic_partition.time_unit" = "DAY"""))
        assertTrue(result[0][1].contains("""dynamic_partition.start" = "-30"""))


        sql """ set global enable_audit_plugin = false; """     
    }
    test_audit_log()


// admin show frontend config
//show frontend config
// admin set frontend config($key = $value)
// set global $key = $value
// show global variables like '%$key'
//show variables like "%version_comment%";
    def test_config = {
    
        List<List<Object>> result = sql """ 
            admin show frontend config 
        """
        logger.info("result = ${result}" )

        def x = 0;

        def val = 0;

        for(int i = 0 ;i<result.size();i++) {
            if (result[i][0] == "query_metadata_name_ids_timeout"){
                x = 1
                val =  result[i][1].toBigInteger() + 2 
                assertTrue( result[i][2] =="long" )
                assertTrue( result[i][3] =="true" )
                assertTrue( result[i][4] == "false")
            }
        }
        assertTrue(x == 1);

        
        sql """ admin set frontend config("query_metadata_name_ids_timeout"= "${val}")"""
        result = sql """ 
            admin show frontend config 
        """
        logger.info("result = ${result}" )

        x = 0 
        for(int i = 0 ;i<result.size();i++) {
            if (result[i][0] == "query_metadata_name_ids_timeout"){
                x = 1
                assertTrue( result[i][1] =="${val}" )
                assertTrue( result[i][2] =="long" )
                assertTrue( result[i][3] =="true" )
                assertTrue( result[i][4] == "false")
            }
        }
        assertTrue(x == 1);
    
        val -= 2 
        sql """ admin set frontend config("query_metadata_name_ids_timeout"= "${val}")"""
        logger.info("result = ${result}" )

        
        x = 0
        result = sql """ show global variables like "create_table_partition_max_num" """
        logger.info("result = ${result}" )

        assert(result[0][0] == "create_table_partition_max_num")
        val = result[0][1].toBigInteger() + 1 ; 
        assert(result[0][2] == "10000")
        sql """ set global create_table_partition_max_num = ${val} """ 
        result = sql """ show global variables like "create_table_partition_max_num" """
        logger.info("result = ${result}" )

        assert(result[0][1].toBigInteger() == val)        
        val -= 1
        sql """ set global create_table_partition_max_num = ${val} """ 
        logger.info("result = ${result}" )

        result = sql """  show frontend config """
        x  = 0  
        for(int i = 0 ;i<result.size();i++) {
            if (result[i][0] == "edit_log_type") {
                assertTrue( result[i][1] =="bdb" )
                assertTrue( result[i][2] =="String")
                assertTrue( result[i][3] =="false" )
                assertTrue( result[i][4] == "false")
                x = 1
            }
        }
        assert(x == 1)


        result = sql """ show variables like "%version_comment%"; """ 
        assertTrue(result.size() == 1)
        assertTrue(result[0][0] == "version_comment")
    }
    test_config();


    logger.info("test_manager_interface_1 end")
}
