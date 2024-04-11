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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("alter_ttl_error") {
    sql """ use @regression_cluster_name1 """
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="-1") """
    String[][] backends = sql """ show backends """
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendHttpPort = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true") && backend[19].contains("regression_cluster_name1")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendHttpPort.put(backend[0], backend[4])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    assertEquals(backendIdToBackendIP.size(), 1)

    sql new File("""${context.file.parent}/../ddl/customer_ttl_delete.sql""").text


    try {
        sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="abcasfaf") """
    try {
        sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="9223372036854775810") """
    try {
        sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="3600") """
    sql (new File("""${context.file.parent}/../ddl/customer_ttl.sql""").text + ttlProperties)

    try {
        sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="-2414") """
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    try {
        sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="abs-") """
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }

    try {
        sql """ ALTER TABLE customer_ttl SET ("file_cache_ttl_seconds"="9223372036854775810") """
        assertTrue(false)
    } catch (Exception e) {
        assertTrue(true)
    }
}
