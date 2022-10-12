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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/window_functions
// and modified by Doris.

import groovy.json.JsonSlurper

suite("show_policy") {
    def storage_exist = { name ->
        def show_storage_policy = sql """
        SHOW STORAGE POLICY;
        """
        for(iter in show_storage_policy){
            if(name == iter[0]){
                return true;
            }
        }
        return false;
    }

    if (!storage_exist.call("showPolicy_1_policy")){
        def create_s3_resource = try_sql """
            CREATE RESOURCE "showPolicy_1_resource"
            PROPERTIES(
                "type"="s3",
                "s3_region" = "bj",
                "s3_endpoint" = "http://bj.s3.comaaaa",
                "s3_root_path" = "path/to/rootaaaa",
                "s3_secret_key" = "aaaa",
                "s3_access_key" = "bbba",
                "s3_bucket" = "test-bucket"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY showPolicy_1_policy
            PROPERTIES(
            "storage_resource" = "showPolicy_1_resource",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
    }
    def show_result = sql """
        SHOW STORAGE POLICY;
    """

    def jsonSlurper = new JsonSlurper()
    if (show_result.size != 0){
        def json_ret = jsonSlurper.parseText(show_result[0][5])
        assertEquals(json_ret["s3_secret_key"], "******")
    }
    assertEquals(storage_exist.call("showPolicy_1_policy"), true)
}
