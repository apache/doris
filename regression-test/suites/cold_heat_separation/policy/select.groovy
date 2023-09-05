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

suite("show_policy") {
    def get_storage_policy = { name ->
        def show_storage_policy = sql """
        SHOW STORAGE POLICY;
        """
        for(policy in show_storage_policy){
            if(name == policy[0]){
                return policy;
            }
        }
        return [];
    }

    if (get_storage_policy.call("showPolicy_1_policy").isEmpty()){
        def create_s3_resource = try_sql """
            CREATE RESOURCE "showPolicy_1_resource"
            PROPERTIES(
                "type"="s3",
                "AWS_REGION" = "bj",
                "AWS_ENDPOINT" = "bj.s3.comaaaa",
                "AWS_ROOT_PATH" = "path/to/rootaaaa",
                "AWS_SECRET_KEY" = "aaaa",
                "AWS_ACCESS_KEY" = "bbba",
                "AWS_BUCKET" = "test-bucket",
                "s3_validity_check" = "false"
            );
        """
        def create_succ_1 = try_sql """
            CREATE STORAGE POLICY showPolicy_1_policy
            PROPERTIES(
            "storage_resource" = "showPolicy_1_resource",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
        def create_succ_2 = try_sql """
            CREATE STORAGE POLICY showPolicy_2_policy
            PROPERTIES(
            "storage_resource" = "showPolicy_1_resource",
            "cooldown_datetime" = "2022-06-08 00:00:00"
            );
        """
    }
    sql """
    CREATE TABLE IF NOT EXISTS create_table_use_created_policy 
    (
        k1 BIGINT,
        date DATE
    )
    DUPLICATE KEY(k1)
    PARTITION BY RANGE(`date`)
    (
        PARTITION `p201701` VALUES LESS THAN ("2017-02-01") ("storage_policy" = "showPolicy_1_policy"),
        PARTITION `p201702` VALUES LESS THAN ("2017-03-01") ("storage_policy" = "showPolicy_2_policy"),,
        PARTITION `p201703` VALUES LESS THAN ("2017-04-01")
    )
    DISTRIBUTED BY HASH (k1) BUCKETS 1
    PROPERTIES(
        "replication_num" = "1"
    )
    """

    def policies = sql """
    show storage policy
    """
    def policyId1 = "0";
    def policyId2 = "0";

    for (pol in policies) {
        if (pol[0] == "showPolicy_1_policy") {
            policyId1 = pol[1];
        }
        if (pol[0] == "showPolicy_2_policy") {
            policyId2 = pol[1];
        }
    }

    assertTrue(policyId1 != "0")
    assertTrue(policyId2 != "0")

    // wait for the tablet report to be done
    sleep(300000)

    def partitions = sql """
    select * from storage_policy("storage_policy" = '${policyId1}')
    """

    assertEquals(partitions.size(), 1)
    
    sql """
    DROP TABLE create_table_use_created_policy FORCE
    """

    sql """
    DROP STORAGE POLICY showPolicy_1_policy;
    """
    sql """
    DROP STORAGE POLICY showPolicy_2_policy;
    """

    sql """
    DROP RESOURCE "showPolicy_1_resource";
    """
}
