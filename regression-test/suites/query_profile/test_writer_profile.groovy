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

import org.apache.doris.regression.action.ProfileAction

suite('test_writer_profile', "nonConcurrent") {
    //cloud-mode
    if (isCloudMode()) {
        return
    }

    sql "set enable_profile=true;"
    sql "set profile_level=1;"

    def s3Endpoint = getS3Endpoint()
    def s3Region = getS3Region()
    def ak = getS3AK()
    def sk = getS3SK()
    def s3Uri = "s3://${getS3BucketName()}/load/data by line.json"

    sql "drop table if exists t;"
    sql """
        CREATE TABLE t(
            a INT,
            b INT
        )
        DUPLICATE KEY(a)
        PROPERTIES("replication_num" = "1");
    """

    // The INSERTs below run on the master FE, which keeps their profiles. ProfileAction reads them
    // with HttpCliAction, which uses HTTPS when the regression config sets enableTLS.
    def master = sql_return_maparray("SHOW FRONTENDS").find { it.IsMaster.toString() == "true" }
    assertNotNull(master, "Cannot locate the master FE")
    def profileAction = new ProfileAction(context,
            new InetSocketAddress(master.Host.toString(), master.HttpPort.toString().toInteger()))

    // BE reports the writer profiles after the INSERT returns, so wait until the profile of this
    // INSERT, found by its marker, is COMPLETE before checking which writer details it has.
    def insertAndGetProfile = {
        def marker = "test_writer_profile_${UUID.randomUUID().toString().replace('-', '')}"
        sql """
            /* ${marker} */ INSERT INTO t
            SELECT * FROM S3(
                "uri" = "$s3Uri",
                "s3.access_key" = "$ak",
                "s3.secret_key" = "$sk",
                "s3.endpoint" = "${s3Endpoint}",
                "s3.region" = "${s3Region}",
                "format" = "json",
                "read_json_by_line" = "true"
            );
        """
        def profileString = profileAction.getProfileBySql(marker, [], 60000L, 500L)
        logger.info(profileString)
        return profileString
    }

    try {
        def profileString = insertAndGetProfile()
        assertFalse(profileString.contains("DeltaWriterV2"), "should not contain DeltaWriterV2")
        assertFalse(profileString.contains("MemTableWriter"), "should not contain MemTableWriter")
    } finally {
        sql "set enable_profile=false;"
    }

    sql "set enable_profile=true;"
    sql "set profile_level=2;"

    try {
        def profileString = insertAndGetProfile()
        assertTrue(profileString.contains("DeltaWriterV2"), "should contain DeltaWriterV2")
        assertTrue(profileString.contains("MemTableWriter"), "should contain MemTableWriter")
    } finally {
        sql "set enable_profile=false;"
        sql "set profile_level=1;"
    }
}
