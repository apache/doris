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


import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request

// Requires a homogeneous WITH_PAIMON_CPP build and debug points enabled on all BEs.
// Opt in explicitly: ordinary JNI regression environments must not silently claim native coverage.
suite("test_paimon_cpp_write_recovery", "p0,external,paimon,nonConcurrent") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonTest"))
            || !"true".equalsIgnoreCase(context.config.otherConfigs.get("enablePaimonCppTest"))) {
        logger.info("Skip native Paimon recovery: enablePaimonTest and enablePaimonCppTest are required")
        return
    }
    String ip = context.config.otherConfigs.get("externalEnvIp")
    String port = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog = "test_paimon_cpp_recovery_catalog"
    String db = "test_paimon_cpp_recovery_db"
    String prefix = "wh/${db}.db/t_native/"
    def points = ["CppPaimonWriteBackend.prepare.serialize_oom",
                  "CppPaimonWriteBackend.close.inject_failure",
                  "PaimonTableWriter.close.store_messages_oom"]
    def client = AmazonS3ClientBuilder.standard()
            .withEndpointConfiguration(new EndpointConfiguration("http://${ip}:${port}", "us-east-1"))
            .withPathStyleAccessEnabled(true)
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("admin", "password")))
            .build()
    def dataFiles = {
        def keys = new TreeSet<String>()
        String token = null
        while (true) {
            def page = client.listObjectsV2(new ListObjectsV2Request()
                    .withBucketName("warehouse").withPrefix(prefix).withContinuationToken(token))
            page.objectSummaries.each { if (it.key.endsWith(".parquet")) keys.add(it.key) }
            if (!page.truncated) break
            token = page.nextContinuationToken
        }
        return keys
    }
    try {
        sql """DROP CATALOG IF EXISTS ${catalog}"""
        sql """
            CREATE CATALOG ${catalog} PROPERTIES (
                'type'='paimon', 'paimon.catalog.type'='filesystem',
                'warehouse'='s3://warehouse/wh',
                's3.endpoint'='http://${ip}:${port}', 's3.region'='us-east-1',
                's3.access_key'='admin', 's3.secret_key'='password',
                's3.path.style.access'='true'
            )
        """
        sql """SWITCH ${catalog}"""
        sql """DROP DATABASE IF EXISTS ${db} FORCE"""
        sql """CREATE DATABASE ${db}"""
        sql """USE ${db}"""
        sql """
            CREATE TABLE t_native (id INT, payload STRING) ENGINE=paimon
            PROPERTIES ('bucket'='-1', 'file.format'='parquet', 'write-only'='true')
        """
        sql """
            CREATE TABLE t_fallback (id INT, payload STRING) ENGINE=paimon
            PROPERTIES ('bucket'='-1', 'file.format'='orc', 'write-only'='true')
        """
        sql "SET enable_paimon_cpp_writer=true"
        explain {
            sql "INSERT INTO t_native VALUES (1, 'native')"
            contains "backend: CPP"
        }
        // This must produce NON-EMPTY C++ serializer-v12 messages decoded/committed by Java FE.
        sql "INSERT INTO t_native VALUES (1, 'native'), (2, NULL)"
        assertEquals([[1, "native"], [2, null]], sql("SELECT * FROM t_native ORDER BY id"))
        assertTrue(!dataFiles().isEmpty())
        spark_paimon """REFRESH TABLE paimon.${db}.t_native"""
        assertSparkDorisResultEquals(
                spark_paimon("SELECT * FROM paimon.${db}.t_native ORDER BY id"),
                sql("SELECT * FROM t_native ORDER BY id"))

        // Check physical objects, not only committed snapshots: an invisible orphan is also a failure.
        int nextId = 3
        points.each { point ->
            def before = dataFiles()
            try {
                GetDebugPoint().enableDebugPointForAllBEs(point)
                test {
                    sql "INSERT INTO t_native VALUES (999, 'must-be-cleaned')"
                    exception(point.endsWith("_oom") ?
                            "Paimon write allocation failed" : "Injected Paimon native close failure")
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(point)
            }
            for (int retry = 0; retry < 50 && dataFiles() != before; ++retry) Thread.sleep(100)
            assertEquals(before, dataFiles())
            assertEquals([[0]], sql("SELECT COUNT(*) FROM t_native WHERE id=999"))
            sql "INSERT INTO t_native VALUES (${nextId}, 'after-failure')"
            assertEquals([[1]], sql("SELECT COUNT(*) FROM t_native WHERE id=${nextId}"))
            ++nextId
        }
        explain {
            sql "INSERT INTO t_fallback VALUES (1, 'jni')"
            contains "backend: JNI"
        }
        sql "INSERT INTO t_fallback VALUES (1, 'jni')"
        assertEquals([[1, "jni"]], sql("SELECT * FROM t_fallback"))
    } finally {
        try {
            points.each { GetDebugPoint().disableDebugPointForAllBEs(it) }
            sql "SET enable_paimon_cpp_writer=false"
            sql """DROP DATABASE IF EXISTS ${catalog}.${db} FORCE"""
            sql """SWITCH internal"""
            sql """DROP CATALOG IF EXISTS ${catalog}"""
        } finally {
            client.shutdown()
        }
    }
}
