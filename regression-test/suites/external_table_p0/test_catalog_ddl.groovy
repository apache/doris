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

suite("test_catalog_ddl", "p0,external,external_docker") {
        String catalog1 = "test_ddl_ctr1";
        // This is only for testing catalog ddl syntax
        sql """drop catalog if exists ${catalog1};"""

        sql """
            create catalog if not exists ${catalog1} comment 'create_comment' properties(
            "type"="es",
            "hosts"="http://10.10.10.10:8888",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
            );
        """

        def result = sql """show create catalog ${catalog1};"""
        assertEquals(result.size(), 1)
        assertTrue(result[0][1].contains("COMMENT \"create_comment\""))

        // can not update comment by property
        sql """ALTER CATALOG ${catalog1} SET PROPERTIES ("comment" = "prop_comment");"""
        result = sql """show create catalog ${catalog1};"""
        assertEquals(result.size(), 1)
        assertTrue(result[0][1].contains("COMMENT \"create_comment\""))

        //update comment
        sql """ALTER CATALOG ${catalog1} MODIFY COMMENT "alter_comment";"""
        result = sql """show create catalog ${catalog1};"""
        assertEquals(result.size(), 1)
        assertTrue(result[0][1].contains("COMMENT \"alter_comment\""))

        sql """drop catalog ${catalog1}"""
}
