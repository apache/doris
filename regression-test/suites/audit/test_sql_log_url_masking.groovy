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

import org.apache.doris.regression.suite.ClusterOptions

import java.nio.charset.StandardCharsets

suite("test_sql_log_url_masking", "docker") {
    def verifyLogs = { String logPath ->
        File logFile = new File(logPath)
        assertTrue(logFile.isFile(), "FE SQL log file must exist")
        String originalMode = (sql "SHOW VARIABLES LIKE 'sql_mode'")[0][1].toString()
        try {
            for (String mode : ["", "NO_BACKSLASH_ESCAPES"]) {
                sql "SET sql_mode='${mode}'"
                for (String prefix : ["layout", "😀😀 layout"]) {
                    String marker = "url_mask_${UUID.randomUUID().toString().replace('-', '')}"
                    long offset = logFile.length()
                    // ALTER of a missing catalog fails before a connector can open any fixture URL.
                    String statement = """/* ${marker} ${prefix} */
ALTER CATALOG missing_${marker} SET PROPERTIES (
  'driver_url' /* keep */ = 'https://example.invalid/it''s.jar?token=fixture-download-token&mode=read',
  'uri' = 'https://reader:fixture-userinfo@example.invalid/data?%74oken=fixture-url-token&region=east',
  'endpoint' = 'https://example.invalid/path?X-Amz-Signature=fixture-signature',
  'warehouse' = 'abfss://container@account.dfs.core.windows.net/path?sig=fixture-sas',
  'connection' = 'jdbc:fixture://example.invalid/db;password=fixture-jdbc',
  'comment' = 'keep\\path and doubled ''quote'''
)"""
                    String expected = statement.replace("token=fixture-download-token", "token=*XXX")
                            .replace("reader:fixture-userinfo@", "")
                            .replace("%74oken=fixture-url-token", "%74oken=*XXX")
                            .replace("https://example.invalid/path?X-Amz-Signature=fixture-signature", "*XXX")
                            .replace("abfss://container@account.dfs.core.windows.net/path?sig=fixture-sas", "*XXX")
                            .replace("jdbc:fixture://example.invalid/db;password=fixture-jdbc", "*XXX")
                    test {
                        sql statement
                        exception "No catalog found with name"
                    }
                    def records = []
                    awaitUntil(30) {
                        String appended = new RandomAccessFile(logFile, "r").withCloseable { input ->
                            input.seek(offset)
                            byte[] bytes = new byte[(int) (input.length() - offset)]
                            input.readFully(bytes)
                            return new String(bytes, StandardCharsets.UTF_8)
                        }
                        records = appended.split(/(?m)(?=^\d{4}-\d{2}-\d{2} )/).findAll { it.contains(marker) }
                        return ["INFO", "DEBUG"].every { level -> records.any { it.contains(" ${level} ") } }
                    }
                    for (String level : ["INFO", "DEBUG"]) {
                        def entries = records.findAll { it.contains(" ${level} ") }
                        assertTrue(entries.any { it.contains(expected) }, "${level} must retain masked SQL layout")
                        for (String secret : ["fixture-download-token", "fixture-userinfo", "fixture-url-token",
                                "fixture-signature", "fixture-sas", "fixture-jdbc"]) {
                            assertFalse(entries.any { it.contains(secret) }, "${level} must redact ${secret}")
                        }
                    }
                }
            }
        } finally {
            sql "SET sql_mode='${originalMode}'"
        }
    }
    String localLog = context.config.otherConfigs.get("feSqlLogFile")
    if (localLog != null) {
        // An owned local cluster may run the same assertions without Docker.
        verifyLogs(localLog)
    } else {
        def options = new ClusterOptions(cloudMode: false, feNum: 1, beNum: 1)
        options.feConfigs += ["enable_print_request_before_execution=true",
                "sys_log_verbose_modules=org.apache.doris.qe.StmtExecutor"]
        docker(options) {
            verifyLogs(cluster.getMasterFe().getLogFilePath())
        }
    }
}
