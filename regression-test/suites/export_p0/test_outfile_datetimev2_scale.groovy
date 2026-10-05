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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_outfile_datetimev2_scale") {
    StringBuilder strBuilder = new StringBuilder()
    strBuilder.append("curl --location-trusted -u " + context.config.jdbcUser + ":" + context.config.jdbcPassword)
    if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
        strBuilder.append(" https://" + context.config.feHttpAddress + "/rest/v1/config/fe")
        strBuilder.append(" --cert " + context.config.otherConfigs.get("trustCert") + " --cacert " + context.config.otherConfigs.get("trustCACert") + " --key " + context.config.otherConfigs.get("trustCAKey"))
    } else {
        strBuilder.append(" http://" + context.config.feHttpAddress + "/rest/v1/config/fe")
    }

    String command = strBuilder.toString()
    def process = command.execute()
    def code = process.waitFor()
    def err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())))
    def out = process.getText()
    logger.info("Request FE Config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def response = parseJson(out.trim())
    assertEquals(response.code, 0)
    assertEquals(response.msg, "success")
    def configJson = response.data.rows
    boolean enableOutfileToLocal = false
    for (Object conf: configJson) {
        assert conf instanceof Map
        if (((Map<String, String>) conf).get("Name").toLowerCase() == "enable_outfile_to_local") {
            enableOutfileToLocal = ((Map<String, String>) conf).get("Value").toLowerCase() == "true"
        }
    }
    if (!enableOutfileToLocal) {
        logger.warn("Please set enable_outfile_to_local to true to run test_outfile_datetimev2_scale")
        return
    }

    def tableName = "test_outfile_datetimev2_scale"
    def uuid = UUID.randomUUID().toString()
    def outFilePath = """/tmp/test_outfile_datetimev2_scale_${uuid}"""

    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE ${tableName} (
                `id` INT,
                `dt0` DATETIMEV2(0),
                `dt3` DATETIMEV2(3),
                `dt6` DATETIMEV2(6)
            )
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES("replication_num" = "1");
        """
        sql """
            INSERT INTO ${tableName} VALUES
            (1, '2026-06-06 15:54:51', '2026-06-06 15:54:51.442', '2026-06-06 15:54:51.442000');
        """

        File path = new File(outFilePath)
        if (!path.exists()) {
            assert path.mkdirs()
        } else {
            throw new IllegalStateException("""${outFilePath} already exists! """)
        }

        sql """
            SELECT * FROM ${tableName} ORDER BY id
            INTO OUTFILE "file://${outFilePath}/"
            FORMAT AS CSV
            PROPERTIES("column_separator" = ",");
        """

        File[] files = path.listFiles().findAll { it.isFile() && it.getName().endsWith(".csv") } as File[]
        assertEquals(1, files.length)

        List<String> outLines = Files.readAllLines(Paths.get(files[0].getAbsolutePath()), StandardCharsets.UTF_8)
        assertEquals(1, outLines.size())
        assertEquals("1,2026-06-06 15:54:51,2026-06-06 15:54:51.442,2026-06-06 15:54:51.442000", outLines.get(0))
        assertFalse(outLines.get(0).contains("2026-06-06 15:54:51.442000,2026-06-06 15:54:51.442000"))
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
        File path = new File(outFilePath)
        if (path.exists()) {
            for (File f: path.listFiles()) {
                f.delete()
            }
            path.delete()
        }
    }
}
