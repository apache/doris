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

suite("test_nereids_show_resources") {
    String jdbcUrl = context.config.jdbcUrl
    def tokens = context.config.jdbcUrl.split('/')
    jdbcUrl=tokens[0] + "//" + tokens[2] + "/" + "?"
    String jdbcUser = context.config.jdbcUser
    String jdbcPassword = context.config.jdbcPassword
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-java-8.0.25.jar"

    String resource_name = "test_resource_for_show_resources"
    String url=tokens[0] + "//" + tokens[2] + "/" + "?"

    sql """ drop RESOURCE if exists test_resource_for_show_resources """
    sql """ CREATE RESOURCE test_resource_for_show_resources PROPERTIES(
                "user" = "${jdbcUser}",
                "type" = "jdbc",
                "password" = "${jdbcPassword}",
                "jdbc_url" = "${url}",
                "driver_url" = "${driver_url}",
                "driver_class" = "com.mysql.cj.jdbc.Driver"
    )"""

    checkNereidsExecute("SHOW RESOURCES")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources'")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' AND ResourceType = 'jdbc'")
    checkNereidsExecute("SHOW RESOURCES WHERE ResourceType = 'jdbc'")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME like 'test_resource_for_show_resources%'")
    checkNereidsExecute("SHOW RESOURCES LIKE 'test_resource_for_show_resources%'")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY Name")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY Name LIMIT 1")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY Name LIMIT 2,1")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY ResourceType")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY ResourceType LIMIT 1")
    checkNereidsExecute("SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY ResourceType LIMIT 1,2")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Name")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Name LIMIT 1")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Name LIMIT 1,2")
    checkNereidsExecute("SHOW RESOURCES ORDER BY ResourceType")
    checkNereidsExecute("SHOW RESOURCES ORDER BY ResourceType LIMIT 1")
    checkNereidsExecute("SHOW RESOURCES ORDER BY ResourceType LIMIT 1,2")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Item")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Item LIMIT 1")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Item LIMIT 1,2")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Value")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Value LIMIT 1")
    checkNereidsExecute("SHOW RESOURCES ORDER BY Value LIMIT 1,2")

    def res1 = sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources'"""
    def size1 = res1.size()
    assertEquals("test_resource_for_show_resources", res1.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res1.get(size1 - 1).get(0))
    assertEquals("jdbc", res1.get(0).get(1))
    assertEquals("jdbc", res1.get(size1 - 1).get(1))

    def res2 = sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' AND ResourceType = 'jdbc'"""
    def size2 = res2.size()
    assertEquals("test_resource_for_show_resources", res2.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res2.get(size2 - 1).get(0))
    assertEquals("jdbc", res2.get(0).get(1))
    assertEquals("jdbc", res2.get(size2 - 1).get(1))

    def res3 = sql """SHOW RESOURCES WHERE NAME like 'test_resource_for_show_resources%'"""
    def size3 = res3.size()
    assertEquals("test_resource_for_show_resources", res3.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res3.get(size3 - 1).get(0))
    assertEquals("jdbc", res3.get(0).get(1))
    assertEquals("jdbc", res3.get(size3 - 1).get(1))

    def res4 = sql """SHOW RESOURCES like 'test_resource_for_show_resources%'"""
    def size4 = res4.size()
    assertEquals("test_resource_for_show_resources", res4.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res4.get(size4 - 1).get(0))
    assertEquals("jdbc", res4.get(0).get(1))
    assertEquals("jdbc", res4.get(size4 - 1).get(1))

    def res5 = sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY Name"""
    def size5 = res5.size()
    assertEquals("test_resource_for_show_resources", res5.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res5.get(size5 - 1).get(0))
    assertEquals("jdbc", res5.get(0).get(1))
    assertEquals("jdbc", res5.get(size5 - 1).get(1))

    def res6 = sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY Name LIMIT 2"""
    def size6 = res6.size()
    assertEquals("test_resource_for_show_resources", res6.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res6.get(size6 - 1).get(0))
    assertEquals("jdbc", res6.get(0).get(1))
    assertEquals("jdbc", res6.get(size6 - 1).get(1))

    def res7 = sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY Name LIMIT 2,2"""
    def size7 = res7.size()
    assertEquals("test_resource_for_show_resources", res7.get(0).get(0))
    assertEquals("test_resource_for_show_resources", res7.get(size7 - 1).get(0))
    assertEquals("jdbc", res7.get(0).get(1))
    assertEquals("jdbc", res7.get(size7 - 1).get(1))

    assertThrows(Exception.class, {
        sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' AND Item = 'user'"""
    })
    assertThrows(Exception.class, {
        sql """SHOW RESOURCES WHERE Item = 'user'"""
    })
    assertThrows(Exception.class, {
        sql """SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' OR ResourceType = 'jdbc'"""
    })
    assertThrows(Exception.class, {
        sql """SHOW RESOURCES WHERE ResourceType like 'jdbc%'"""
    })
    assertThrows(Exception.class, {
        sql """"SHOW RESOURCES WHERE NAME = 'test_resource_for_show_resources' ORDER BY 1"""
    })
}
