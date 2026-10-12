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

suite("test_catalog_ddl", "p0,external") {
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

        String weightedCatalog = "test_ddl_weighted_meta_cache"
        sql """drop catalog if exists ${weightedCatalog}"""
        sql """
            create catalog ${weightedCatalog} properties(
                "type" = "hms",
                "hive.metastore.uris" = "thrift://127.0.0.1:9083",
                "meta.cache.max-weight" = "128MB",
                "meta.cache.hive.file.max-weight" = "64MB"
            )
        """
        result = sql """show create catalog ${weightedCatalog}"""
        assertEquals(result.size(), 1)
        assertTrue(result[0][1].contains("\"meta.cache.max-weight\" = \"128MB\""))
        assertTrue(result[0][1].contains("\"meta.cache.hive.file.max-weight\" = \"64MB\""))

        sql """
            alter catalog ${weightedCatalog} set properties(
                "meta.cache.max-weight" = "96MB",
                "meta.cache.hive.file.max-weight" = "48MB"
            )
        """
        result = sql """show create catalog ${weightedCatalog}"""
        assertTrue(result[0][1].contains("\"meta.cache.max-weight\" = \"96MB\""))
        assertTrue(result[0][1].contains("\"meta.cache.hive.file.max-weight\" = \"48MB\""))

        test {
            sql """
                alter catalog ${weightedCatalog} set properties(
                    "meta.cache.default.schema.max-weight" = "invalid"
                )
            """
            exception "Invalid cache weight for 'meta.cache.default.schema.max-weight': invalid"
        }
        test {
            sql """alter catalog ${weightedCatalog} set properties(
                "meta.cache.iceberg.partiton.max-weight" = "64MB")"""
            exception "Unknown metadata cache weight property: meta.cache.iceberg.partiton.max-weight"
        }
        sql """drop catalog ${weightedCatalog}"""

        test {
            sql """
                create catalog ${weightedCatalog} properties(
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
                    "meta.cache.max-weight" = "64MB",
                    "meta.cache.hive.file.max-weight" = "128MB"
                )
            """
            exception "meta.cache.hive.file.max-weight can not exceed meta.cache.max-weight"
        }

        test {
            sql """
                create catalog ${weightedCatalog} properties(
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
                    "meta.cache.max-weight" = "10%"
                )
            """
            exception "Invalid cache weight for 'meta.cache.max-weight': 10%"
        }

        test {
            sql """
                create catalog ${weightedCatalog} properties(
                    "type" = "es",
                    "hosts" = "http://10.10.10.10:8888",
                    "meta.cache.max-weight" = "invalid"
                )
            """
            exception "Invalid cache weight for 'meta.cache.max-weight': invalid"
        }

        // Newly submitted unknown entries must not silently disable the requested memory limit.
        sql """drop catalog if exists test_ddl_future_hive_cache"""
        test {
            sql """
                create catalog test_ddl_future_hive_cache properties(
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
                    "meta.cache.hive.future_entry.max-weight" = "64MB"
                )
            """
            exception "Unknown metadata cache weight property: meta.cache.hive.future_entry.max-weight"
        }
        test {
            sql """
                create catalog ${weightedCatalog} properties(
                    "type" = "hms",
                    "hive.metastore.uris" = "thrift://127.0.0.1:9083",
                    "meta.cache.iceberg.manifest.max-weight" = "invalid"
                )
            """
            exception "Invalid cache weight for 'meta.cache.iceberg.manifest.max-weight': invalid"
        }

        sql """drop catalog if exists test_ddl_future_core_cache"""
        sql """
            create catalog test_ddl_future_core_cache properties(
                "type" = "es",
                "hosts" = "http://10.10.10.10:8888",
                "meta.cache.default.future_entry.max-weight" = "64MB"
            )
        """
        sql """alter catalog test_ddl_future_core_cache set properties("comment" = "updated")"""
}
