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

suite('test_manager_interface_3',"p0") {


    String jdbcUrl = context.config.jdbcUrl
    def tokens = context.config.jdbcUrl.split('/')
    jdbcUrl=tokens[0] + "//" + tokens[2] + "/" + "?"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"
//create role $role_name
//drop role $role_name
// create user $user_name identified by "$password" default role "$role_name"
// grant $privileges on $privilege_level to $user_name
// grant $privileges on $privilege_level to role $role_name
// grant "$role_name" to "$user_name"
// revoke $privileges on $privilege_level from "$user_name"
// revoke $privileges on $privilege_level from role $role_name
// set password for $user = PASSWORD("$password")
// revoke "$role_name" from "$user_name"
// show all grants
// show grants
// show roles
    def test_role_grant  = {
        def user1 = 'test_manager_role_grant_user1'
        def user2 = 'test_manager_role_grant_user2'
        def role1 = 'test_manager_role_grant_role1'
        def pwd = '123456'
        def new_pwd = 'Ab1234567^'
        def dbName = 'test_manager_role_grant_db'
        def dbName2 = 'test_manager_role_grant_db2'

        def tbName = 'test_manager_tb'
        
        def url=tokens[0] + "//" + tokens[2] + "/" + dbName + "?"

        sql """drop user if exists ${user1}"""
        sql """drop user if exists ${user2}"""
        sql """drop role if exists ${role1}"""
        sql """DROP DATABASE IF EXISTS ${dbName}"""
        sql """DROP DATABASE IF EXISTS ${dbName2}"""
        
        sql """CREATE DATABASE ${dbName}"""
        sql """CREATE DATABASE ${dbName2}"""

        sql """ create table ${dbName}.${tbName}(
                    k1 TINYINT,
                    k2 CHAR(10) COMMENT "string column"
                ) COMMENT "manager_test_table"
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ('replication_num' = '1',
                "bloom_filter_columns" = "k2"
                ) """ 

        sql """ insert into ${dbName}.${tbName} values(1,"abc") """
        
        sql """CREATE ROLE ${role1}"""
        sql """grant  SELECT_PRIV on ${dbName} TO ROLE '${role1}' """

        sql """CREATE USER '${user1}' IDENTIFIED BY '${pwd}' default role '${role1}' """
        sql """CREATE USER '${user2}' IDENTIFIED BY '${pwd}'  """

        connect(user1, "${pwd}", url) {
            test {
                sql """ select 1"""
                result(
                    [[1]]
                ) 
            }
            test {
                sql """ select * from ${dbName}.${tbName} """
                result(
                    [[1,"abc"]]
                )
            } 
            test {
                sql """ use ${dbName2} """
                exception "Access denied for user"
            }
            test {
                sql """ Drop table ${dbName}.${tbName} """
                exception "Access denied; you need (at least one of) the (DROP) privilege(s) for this operation"
            }
            test {
                sql """ create table test_manager_tb_2 (
                    k1 TINYINT,
                    k2 CHAR(10) COMMENT "string column"
                ) COMMENT "manager_test_table_2"
                DISTRIBUTED BY HASH(k1) BUCKETS 1;
                """
                exception "Access denied; you need (at least one of) the (CREATE) privilege(s) for this operation"
            }
            test {
                sql """ insert into test_manager_tb values(1,"2"); """
                exception """LOAD command denied to user"""
            }
        }
        sql """grant  DROP_PRIV on ${dbName} TO ROLE '${role1}' """
        sql """grant  CREATE_PRIV on ${dbName} TO  '${user1}' """
        
        connect(user1, "${pwd}", url) {
    
            sql """ create table test_manager_tb_2 (
                    k1 TINYINT,
                    k2 CHAR(10) COMMENT "string column"
                ) COMMENT "manager_test_table_2"
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ('replication_num' = '1');
                """
            sql """  Drop table test_manager_tb_2 """
        }

        sql """grant  LOAD_PRIV on ${dbName} TO  '${user2}' """
        sql """ grant "${role1}" to '${user2}' """  
        connect(user2, "${pwd}", url) {
    
            test {  
                sql """ create table test_manager_tb_2 (
                    k1 TINYINT,
                    k2 CHAR(10) COMMENT "string column"
                ) COMMENT "manager_test_table_2"
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ('replication_num' = '1');
                """
                exception "Access denied; you need (at least one of) the (CREATE) privilege(s) for this operation"
            }            
            test {
                sql """ select * from ${dbName}.${tbName} """
                result(
                    [[1,"abc"]]
                )
            } 

            sql """ insert into ${tbName} values (2,"adc") """
            sql """ insert into ${tbName} values (3,"ttt") """

            test {
                sql """ select * from ${dbName}.${tbName} order by k1"""
                result(
                    [[1,"abc"],[2,"adc"],[3,"ttt"]]
                )
            }

            test {
                sql """ use ${dbName2} """
                exception "Access denied for user"
            }
        }
        
        List<List<Object>> result = sql  """show all grants """        
        def x = 0 
        for(int i = 0;i < result.size(); i++ ) {
            
            // Roles: test_manager_role_grant_role1 
            if ( result[i][3] == "${role1}") {
                //UserIdentity: 
                logger.info("result[${i}][0] = ${result[i][0]}" )
                if (result[i][0].contains("test_manager_role_grant_user1")){
                    //DatabasePrivs 
                    assertTrue(result[i][6] == "internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.test_manager_role_grant_db: Select_priv,Create_priv,Drop_priv")
                    x ++ 
                }else if (result[i][0].contains("test_manager_role_grant_user2")) {
                    assertTrue(result[i][6] == "internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.test_manager_role_grant_db: Select_priv,Load_priv,Drop_priv")
                    x ++ 

                }else {
                    assertTrue(false." only ${user1} and ${user2}, no ${result[i][0]}")
                }
            }
            else if ( result[i][3] =="admin"){
                if (result[i][0] == """'admin'@'%'"""){
                    x++
                }
            
            } else if (result[i][3] =="operator") {
                if (result[i][0] =="""'root'@'%'""" ){
                    x++
                }
            }
        }
        assertTrue(x == 4)
        
        sql """ revoke CREATE_PRIV on ${dbName}  from '${user1}' """ 
        connect(user1, "${pwd}", url) {
            test {  
                sql """ create table test_manager_tb_2 (
                    k1 TINYINT,
                    k2 CHAR(10) COMMENT "string column"
                ) COMMENT "manager_test_table_2"
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ('replication_num' = '1');
                """
                exception "Access denied; you need (at least one of) the (CREATE) privilege(s) for this operation"
            }   
        }

        sql """ revoke LOAD_PRIV on ${dbName}  from '${user2}' """ 
        connect(user2, "${pwd}", url) {
            test{
                sql """ insert into test_manager_tb values(1,"2"); """
                exception """LOAD command denied to user"""
            }
        }

        result = sql  """show all grants """        
        x = 0 
        for(int i = 0;i < result.size(); i++ ) {
            
            // Roles: test_manager_role_grant_role1 
            if ( result[i][3] == "${role1}") {
                //UserIdentity: 
                logger.info("result[${i}][0] = ${result[i][0]}" )
                if (result[i][0].contains("test_manager_role_grant_user1")){
                    //DatabasePrivs 
                    assertTrue(result[i][6] == "internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.test_manager_role_grant_db: Select_priv,Drop_priv")
                    x ++ 
                }else if (result[i][0].contains("test_manager_role_grant_user2")) {
                    assertTrue(result[i][6].contains("internal.information_schema: Select_priv; internal.mysql: Select_priv; internal.test_manager_role_grant_db: Select_priv,Drop_priv"))
                    x ++ 

                }else {
                    assertTrue(false." only ${user1} and ${user2}, no ${result[i][0]}")
                }
            }
            else if ( result[i][3] =="admin"){
                if (result[i][0] == """'admin'@'%'"""){
                    x++
                }
            
            } else if (result[i][3] =="operator") {
                if (result[i][0] =="""'root'@'%'""" ){
                    x++
                }
            }
        }
        assertTrue(x == 4)
        

        result = sql  """show  grants """        
        x = 0 
        for(int i = 0;i < result.size(); i++ ) {
            if (result[i][3] =="operator") {
                if (result[i][0] =="""'root'@'%'""" ){
                    if (result[i][6] == "internal.information_schema: Select_priv; internal.mysql: Select_priv"){
                        assertTrue(result[i][4]=="Node_priv,Admin_priv")
                        x++
                    
                    }
                }
            }
        }
        assertTrue(x == 1)
        
        result = sql  """show  roles """        
        x = 0 
        for(int i = 0;i < result.size(); i++ ) {
            //NAME
            assertTrue(result[i][0].toLowerCase() != "null")

            if (result[i][0] =="test_manager_role_grant_role1") {
                //Users
                assertTrue(result[i][2].contains("test_manager_role_grant_user2'@'%"))
                assertTrue(result[i][2].contains("test_manager_role_grant_user1'@'%"))
                x ++
            }else if (result[i][0] == "admin"){
                assertTrue(result[i][2].contains("admin'@'%"))
                x ++ 
            }else if (result[i][0] == "operator"){
                assertTrue(result[i][2].contains("root'@'%"))
                x++
            }
        }
        assertTrue(x == 3)
        



        sql """revoke  DROP_PRIV on ${dbName} FROM ROLE '${role1}' """
        sql """create table ${dbName}.test_manager_tb_2 (
                    k1 TINYINT,
                    k2 CHAR(10) COMMENT "string column"
                ) COMMENT "manager_test_table_2"
                DISTRIBUTED BY HASH(k1) BUCKETS 1
                PROPERTIES ('replication_num' = '1');"""
        

        connect(user1, "${pwd}", url) {
            test {
                sql """ Drop table ${dbName}.test_manager_tb_2"""
                exception "Access denied; you need (at least one of) the (DROP) privilege(s) for this operation"
            }
        }

        connect(user2, "${pwd}", url) {
            test{
                sql """ Drop table ${dbName}.test_manager_tb_2"""
                exception "Access denied; you need (at least one of) the (DROP) privilege(s) for this operation"
            }
        }

        sql """set password for '${user2}' = password('${new_pwd}')"""
        try {
            connect(user2, '${pwd}', url) {}
            assertTrue(false. "should not be able to login")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
        } 

        connect(user2, "${new_pwd}", url) {            
            result =  sql """ select k1 from ${dbName}.${tbName} order by k1 desc limit 1"""
            assertTrue(result[0][0] == 3) 
        
            
            result =  sql """ select * from ${dbName}.${tbName} order by k1"""
            assertTrue(result[0][0] ==1)
            assertTrue(result[0][1] =="abc")

            assertTrue(result[1][0] ==2)
            assertTrue(result[1][1] =="adc")
            
            assertTrue(result[2][0] ==3)
            assertTrue(result[2][1] =="ttt")
        }

        
        sql """ revoke "${role1}" from "${user2}" """ 

        try {
            connect(user2, '${pwd}', url) {}
            assertTrue(false. "should not be able to login")
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Access denied for user"), e.getMessage())
        } 

        sql """ drop database ${dbName} """ 
        sql """ drop database ${dbName2} """ 

        sql """drop user  ${user1}"""
        sql """drop user  ${user2}"""
        sql """drop role  ${role1}"""

    }   
    test_role_grant()




// grant $privileges on resource $resource_name to $user
// grant $privileges on resource $resource_name to role $role_name    
// revoke $privileges on resource $resource_name from $user_name
    def test_resource = {
        def user = 'test_manager_resource_user'
        def role = 'test_manager_resource_role'
        def resource_name = "test_manager_resource_case"
        def pwd = "123456"
        def url=tokens[0] + "//" + tokens[2] + "/" + "?"
        
        sql """ drop RESOURCE if exists  ${resource_name} """ 
        sql  """ CREATE RESOURCE ${resource_name} PROPERTIES(
                "user" = "${jdbcUser}",
                "type" = "jdbc",
                "password" = "${jdbcPassword}",
                "jdbc_url" = "${url}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver"
        )"""



        sql """drop user if exists ${user}"""
        sql """drop role if exists ${role}"""
        
        sql """CREATE ROLE ${role}"""
        sql """grant  USAGE_PRIV on RESOURCE  ${resource_name} TO ROLE '${role}' """

        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}' default role '${role}' """
        
        List<List<Object>> result = sql  """ show resources """

        def x = 0
        for(int i = 0;i<result.size();i++) {
            assert(result[i][0].toLowerCase() != "null") //Name
            if (result[i][0] == resource_name) {
                x ++
            }
        }
        assertTrue(x == 20)

        connect(user, "${pwd}", url) { 
            result = sql """ show resources """
            x = 0
            for(int i = 0;i<result.size();i++) {
                assert(result[i][0].toLowerCase() != "null") //Name
                if (result[i][0] == resource_name) {
                    x ++
                }
            }
            assertTrue(x == 20)
        }


        result = sql """ show all grants"""
        x = 0 
        for(int i = 0;i < result.size(); i++ ) {
            
            if ( result[i][3] == "${role}") {
                //UserIdentity: 
                if (result[i][0].contains(user)){
                    //DatabasePrivs 
                    assertTrue(result[i][9] == "test_manager_resource_case: Usage_priv")
                    x ++ 
                }
            }
        }
        assertTrue(x == 1)


        sql """ revoke USAGE_PRIV on RESOURCE  ${resource_name} FROM ROLE '${role}' """
        connect(user, "${pwd}", url) { 
            result = sql """ show resources """
            x = 0
            for(int i = 0;i<result.size();i++) {
                assert(result[i][0].toLowerCase() != "null") //Name
                if (result[i][0] == resource_name) {
                    x ++
                }
            }
            assertTrue(x == 0)
        }

        sql """grant  USAGE_PRIV on RESOURCE  ${resource_name} TO '${user}' """
        connect(user, "${pwd}", url) { 
            result = sql """ show resources """
            x = 0
            for(int i = 0;i<result.size();i++) {
                assert(result[i][0].toLowerCase() != "null") //Name
                if (result[i][0] == resource_name) {
                    x ++
                }
            }
            assertTrue(x == 20)
        }
        sql """ drop RESOURCE if exists  ${resource_name} """ 
        sql """drop user if exists ${user}"""
        sql """drop role if exists ${role}"""
        
        /*
        mysql>  show resources where name = "test_manager_resource_case"\G ;
        *************************** 1. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: test_connection
            Value: true
        *************************** 2. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: driver_class
            Value: com.mysql.cj.jdbc.Driver
        *************************** 3. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: connection_pool_max_size
            Value: 10
        *************************** 4. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: connection_pool_max_life_time
            Value: 1800000
        *************************** 5. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: create_time
            Value: 2024-06-04 17:35:19.097481994
        *************************** 6. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: meta_names_mapping
            Value: 
        *************************** 7. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: only_specified_database
            Value: false
        *************************** 8. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: driver_url
            Value: mysql-connector-java-8.0.25.jar
        *************************** 9. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: type
            Value: jdbc
        *************************** 10. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: connection_pool_min_size
            Value: 1
        *************************** 11. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: jdbc_url
            Value: jdbc:mysql://127.0.0.1:55557/?&yearIsDateType=false&tinyInt1isBit=false&useUnicode=true&rewriteBatchedStatements=true&characterEncoding=utf-8
        *************************** 12. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: lower_case_meta_names
            Value: false
        *************************** 13. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: connection_pool_max_wait_time
            Value: 5000
        *************************** 14. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: password
            Value: 
        *************************** 15. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: use_meta_cache
            Value: false
        *************************** 16. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: exclude_database_list
            Value: 
        *************************** 17. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: include_database_list
            Value: 
        *************************** 18. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: checksum
            Value: fdf55dcef04b09f2eaf42b75e61ccc9a
        *************************** 19. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: connection_pool_keep_alive
            Value: false
        *************************** 20. row ***************************
                Name: test_manager_resource_case
        ResourceType: jdbc
                Item: user
            Value: root
        20 rows in set (0.00 sec)
        */

    }
    test_resource()


// show property like '%$resource_tag%'
// show property for $user like '%$resource_tag%'
// set property for $user 'resource_tags.location' = '$tags'
    def test_property = {
        def user = 'test_manager_property_user'
        def pwd = "123456"
        def url=tokens[0] + "//" + tokens[2] + "/" + "?"

        sql """drop user if exists ${user}"""

        sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
        
        connect(user, "${pwd}", url) { 
            List<List<Object>> result = sql """ show property like  "max_query_instances" """
            assertTrue(result[0][0]=="max_query_instances")
            assertTrue(result[0][1]=="-1")
        }

        List<List<Object>> result = sql """ show property for ${user} like  "max_query_instances" """
        assertTrue(result[0][0]=="max_query_instances")
        assertTrue(result[0][1]=="-1")

        sql """ set property for ${user}  'max_query_instances' ="100000"; """
        result = sql """ show property for ${user} like  "max_query_instances" """
        assertTrue(result[0][0]=="max_query_instances")
        assertTrue(result[0][1]=="100000")

        sql """ drop user ${user} """
    }
    test_property() 


}
