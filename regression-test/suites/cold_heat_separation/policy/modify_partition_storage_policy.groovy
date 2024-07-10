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

suite("modify_partition_storage_policy") {
    //  1. create a non-partitioned table
    String tblName = "test_modify_table"

    sql """DROP TABLE IF EXISTS ${tblName} FORCE; """

    sql """
        CREATE TABLE `${tblName}`
        (
            k1 BIGINT,
            k2 LARGEINT,
            v1 VARCHAR(2048)
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH (k1) BUCKETS 3
        PROPERTIES(
            "replication_num" = "1"
        );
    """

    //  2.create resource1, resource2, and policy1 and policy2.
    String resource1 = "test_modify_resource1"
    String resource2 = "test_modify_resource2"

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource1}"
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

    sql """
        CREATE RESOURCE IF NOT EXISTS "${resource2}"
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

    String policy_name1 = "test_modify_policy1"
    String policy_name2 = "test_modify_policy2"
    sql """
            CREATE STORAGE POLICY IF NOT EXISTS `${policy_name1}`
            PROPERTIES(
            "storage_resource" = "${resource1}",
            "cooldown_datetime" = "2999-06-18 00:00:00"
            );
        """

    sql """
            CREATE STORAGE POLICY IF NOT EXISTS `${policy_name2}`
            PROPERTIES(
            "storage_resource" = "${resource2}",
            "cooldown_datetime" = "2999-06-18 00:00:00"
            );
        """

    //  3. add storage policy to the current partition,make sure that the storage information in the metadata of FE and BE be the same
    try_sql """
                ALTER TABLE ${tblName} MODIFY PARTITION (${tblName}) SET("storage_policy"="${policy_name1}");
            """

    def get_policy_id_from_name = { name ->
        def show_storage_policy = sql """SHOW STORAGE POLICY;"""

        for (iter in show_storage_policy) {
            if (name == iter[0]) {
                return iter[1];
            }
        }
        return -1;
    }

    // 3.1  check fe meta
    def partitions = sql """show partitions from ${tblName}"""
    for (par in partitions) {
        // 12th is storage policy name
        assertTrue(par[12] == "${policy_name1}")
    }


    // 3.2 get storage policy from be metadata
    def tablet_result = sql """SHOW TABLETS FROM ${tblName} PARTITION ${tblName};"""
    def metadata_url = tablet_result[0][17]
    def (code, out, err) = curl("GET", metadata_url)
    def json = parseJson(out)
    def be__policy_id1 = Integer.parseInt(json.storage_policy_id.toString())

    // assert is equal: storage id in fe & be meta
    def policy_id = Integer.parseInt(get_policy_id_from_name.call(policy_name1).toString())
    assertEquals(be__policy_id1, policy_id)


    // 4. alter policy with different resources
    try {
        sql """
                ALTER TABLE ${tblName} MODIFY PARTITION (${tblName}) SET("storage_policy"="${policy_name2}");
            """
    }
    catch (Exception e) {
        logger.info(e.getMessage())
    }
    // 4.1 check fe meta
    def partitions2 = sql "show partitions from ${tblName}"
    for (par in partitions2) {
        // 12th is storage policy name
        assertTrue(par[12] == "${policy_name1}")
    }

    // 4.2 check be meta
    def tablet_result2 = sql """SHOW TABLETS FROM ${tblName} PARTITION ${tblName};"""
    def metadata_url2 = tablet_result2[0][17]
    def (code2, out2, err2) = curl("GET", metadata_url2)
    def json2 = parseJson(out2)
    def be_policy_id2 = Integer.parseInt(json2.storage_policy_id.toString())
    assertEquals(be_policy_id2, policy_id)
}
