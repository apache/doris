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

package org.apache.doris.regression.util

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

class MySqlClient {
    // MySQL option files interpret backslash escapes even inside quoted values.
    private static String quoteOption(String value) {
        String text = value ?: ''
        if (text.indexOf(0) >= 0) {
            throw new IllegalArgumentException('MySQL credentials must not contain NUL')
        }
        return '"' + text.replace('\\', '\\\\').replace('"', '\\"')
                .replace('\n', '\\n').replace('\r', '\\r')
                .replace('\t', '\\t').replace('\b', '\\b') + '"'
    }

    static Map execute(String user, String password, List<String> options, String sql,
                       File directory = null, String executable = 'mysql') {
        def credentials = Files.createTempFile('doris-mysql-', '.cnf',
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString('rw-------')))
        Process process = null
        try {
            String config = "[client]\nuser=${quoteOption(user)}\npassword=${quoteOption(password)}\n"
            Files.write(credentials, config.getBytes(StandardCharsets.UTF_8))
            List<String> command = [executable, "--defaults-extra-file=${credentials}".toString()]
            command.addAll(options.collect { it.toString() })
            ProcessBuilder builder = new ProcessBuilder(command)
            if (directory != null) {
                builder.directory(directory)
            }
            process = builder.start()
            StringBuilder stdout = new StringBuilder()
            StringBuilder stderr = new StringBuilder()
            Thread outputReader = process.consumeProcessOutputStream(stdout)
            Thread errorReader = process.consumeProcessErrorStream(stderr)
            process.outputStream.withWriter('UTF-8') { writer -> writer.write(sql) }
            int exitCode = process.waitFor()
            outputReader.join()
            errorReader.join()
            return [exitCode: exitCode, stdout: stdout.toString(), stderr: stderr.toString()]
        } finally {
            try {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly().waitFor()
                }
            } finally {
                Files.deleteIfExists(credentials)
            }
        }
    }
}
