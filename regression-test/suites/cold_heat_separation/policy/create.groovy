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

suite("create_policy") {
    sql """ADMIN SET FRONTEND CONFIG ("enable_storage_policy" = "true");"""

    def has_created_1 = sql """
        SHOW RESOURCES WHERE NAME = "crete_policy_1";
    """

    // doesn't have name crete_policy_1 resources
    // normal
    if(has_created_1.size() == 0) {
        sql """
        CREATE RESOURCE "crete_policy_1"
        PROPERTIES(
            "type" = "s3",
            "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
            "AWS_REGION" = "bj",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket"
        );
        """

        def create_sucess = sql """
             SHOW RESOURCES WHERE NAME = "crete_policy_1";
        """
        assertEquals(create_sucess.size(), 10)

        def failed_cannot_create_duplicate_name_resources = try_sql """
            CREATE RESOURCE "crete_policy_1"
            PROPERTIES(
                "type" = "s3",
                "AWS_ENDPOINT" = "http://bj.s3.comaaaab",
                "AWS_REGION" = "bjc",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_MAX_CONNECTIONS" = "50",
                "AWS_REQUEST_TIMEOUT_MS" = "3000",
                "AWS_CONNECTION_TIMEOUT_MS" = "1000",
                "AWS_BUCKET" = "test-bucket"
            );
        """

        // errCode = 2, detailMessage = Resource(crete_policy_1) already exist
        assertEquals(failed_cannot_create_duplicate_name_resources, null)
    }

    // can't create success, due to missing required items
    def has_created_2 = sql """
        SHOW RESOURCES WHERE NAME = "crete_policy_2";
    """
    if (has_created_2.size() == 0) {
        def failed_create_1 = try_sql """
        CREATE RESOURCE "crete_policy_2"
        PROPERTIES(
            "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
            "AWS_REGION" = "bj",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket"
        );
        """
        // errCode = 2, detailMessage = Resource type can't be null
        assertEquals(failed_create_1, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2"
        PROPERTIES(
            "type" = "s3",
            "AWS_REGION" = "bj",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket"
        );
        """
        // errCode = 2, detailMessage = Missing [AWS_ENDPOINT] in properties.
        assertEquals(failed_create_2, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2"
        PROPERTIES(
            "type" = "s3",
            "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket"
        );
        """
        // errCode = 2, detailMessage = Missing [AWS_REGION] in properties.
        assertEquals(failed_create_2, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
            "AWS_REGION" = "bj",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket"
        );
        """
        // errCode = 2, detailMessage = Missing [AWS_ACCESS_KEY] in properties.
        assertEquals(failed_create_2, null)
    }

    if (has_created_2.size() == 0) {
        def failed_create_2 = try_sql """
        CREATE RESOURCE "crete_policy_2"
        PROPERTIES(
            "type"="s3",
            "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
            "AWS_REGION" = "bj",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_MAX_CONNECTIONS" = "50",
            "AWS_REQUEST_TIMEOUT_MS" = "3000",
            "AWS_CONNECTION_TIMEOUT_MS" = "1000",
            "AWS_BUCKET" = "test-bucket"
        );
        """
        // errCode = 2, detailMessage = Missing [AWS_SECRET_KEY] in properties.
        assertEquals(failed_create_2, null)
    }

    // can create success, because there are default values
    def has_created_3 = sql """
        SHOW RESOURCES WHERE NAME = "crete_policy_3";
    """
    if (has_created_3.size() == 0) {
        def succ_create_3 = try_sql """
        CREATE RESOURCE "crete_policy_3"
        PROPERTIES(
            "type"="s3",
            "AWS_REGION" = "bj",
            "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
            "AWS_ROOT_PATH" = "path/to/rootaaaa",
            "AWS_SECRET_KEY" = "aaaa",
            "AWS_ACCESS_KEY" = "bbba",
            "AWS_BUCKET" = "test-bucket"
        );
        """
        // "AWS_MAX_CONNECTIONS" = "50", "AWS_REQUEST_TIMEOUT_MS" = "3000","AWS_CONNECTION_TIMEOUT_MS" = "1000"
        assertEquals(succ_create_3.size(), 1)
    }

    // create policy


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

    if (!storage_exist.call("testPolicy_10")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_10_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_10
            PROPERTIES(
            "storage_resource" = "testPolicy_10_resource",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
        assertEquals(storage_exist.call("testPolicy_10"), true)
    }

    if (!storage_exist.call("testPolicy_11")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_11_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_11
            PROPERTIES(
            "storage_resource" = "testPolicy_11_resource",
            "cooldown_ttl" = "10086"
            );
        """
        assertEquals(storage_exist.call("testPolicy_11"), true)
    }

    if (!storage_exist.call("testPolicy_12")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_12_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_12
            PROPERTIES(
            "storage_resource" = "testPolicy_12_resource",
            "cooldown_ttl" = "10086",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
        // errCode = 2, detailMessage = cooldown_datetime and cooldown_ttl can't be set together.
        assertEquals(storage_exist.call("testPolicy_12"), false)
    }

    if (!storage_exist.call("testPolicy_13")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_13_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_13
            PROPERTIES(
            "storage_resource" = "testPolicy_13_resource",
            "cooldown_ttl" = "-10086"
            );
        """
        // errCode = 2, detailMessage = cooldown_ttl must >= 0.
        assertEquals(storage_exist.call("testPolicy_13"), false)
    }

    if (!storage_exist.call("testPolicy_14")) {
        def create_s3_resource = try_sql """
            CREATE RESOURCE "testPolicy_14_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "http://bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_14
            PROPERTIES(
            "storage_resource" = "testPolicy_14_resource",
            "cooldown_datetime" = "2022-06-08"
            );
        """
        //  errCode = 2, detailMessage = cooldown_datetime format error: 2022-06-08
        assertEquals(storage_exist.call("testPolicy_14"), false)
    }

    if (!storage_exist.call("testPolicy_15")) {
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY testPolicy_15
            PROPERTIES(
            "storage_resource" = "s3_resource_not_exist",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
        //  errCode = 2, detailMessage = storage resource doesn't exist: s3_resource_not_exist
        assertEquals(storage_exist.call("testPolicy_15"), false)
    }
}
