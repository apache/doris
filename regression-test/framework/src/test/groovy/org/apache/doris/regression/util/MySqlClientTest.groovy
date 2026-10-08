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

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.nio.file.Files
import java.nio.file.Path

import static org.junit.jupiter.api.Assertions.*

class MySqlClientTest {
    @TempDir
    Path tempDir

    private File client(int exitCode) {
        File script = tempDir.resolve('fake mysql client').toFile()
        script.text = """#!/bin/sh
file=\${1#--defaults-extra-file=}
printf '%s\\n' "\$file"
ls -l "\$file"
printf '%s\\n' 'CONFIG'
cat "\$file"
printf '%s\\n' 'ARGS'
printf '%s\\n' "\$@"
printf '%s\\n' 'SQL'
cat
printf '%s\\n' 'DIR'
pwd
printf '%s' 'test diagnostic' >&2
exit ${exitCode}
"""
        assertTrue(script.setExecutable(true))
        return script
    }

    @Test
    void passesOptionsAndSqlLiterallyAndCleansPrivateCredentials() {
        File executable = client(0)
        File directory = Files.createDirectory(tempDir.resolve('working directory')).toFile()
        String sql = 'select "$HOME", "`id`", "\\n";\n'
        ['', null, 'fake secret "\\\\#;\n\t密碼'].each { password ->
            def result = MySqlClient.execute('root', password,
                    ['--ssl-mode=VERIFY_CA', '--ssl-ca=/path with spaces/ca.pem'], sql, directory, executable.path)
            assertEquals(0, result.exitCode)
            assertEquals('test diagnostic\n', result.stderr)
            def lines = result.stdout.readLines()
            assertFalse(new File(lines[0]).exists())
            assertTrue(lines[1].startsWith('-rw-------'))
            String args = result.stdout.split('ARGS\n', 2)[1].split('SQL\n', 2)[0]
            assertEquals(["--defaults-extra-file=${lines[0]}".toString(), '--ssl-mode=VERIFY_CA',
                          '--ssl-ca=/path with spaces/ca.pem'], args.readLines())
            assertTrue(result.stdout.contains('SQL\n' + sql))
            assertTrue(result.stdout.endsWith('DIR\n' + directory.canonicalPath + '\n'))
            String config = result.stdout.split('CONFIG\n', 2)[1].split('ARGS\n', 2)[0]
            assertTrue(config.startsWith('[client]\nuser="root"\npassword="'))
            if (!password) assertTrue(config.endsWith('password=""\n'))
            else assertEquals(3, config.readLines().size())
        }
    }

    @Test
    void preservesNonRootUserAndFailureExitCodeAndStillCleansFile() {
        def result = MySqlClient.execute('load_user', 'fake-secret', [], 'select 1;', null, client(7).path)
        assertEquals(7, result.exitCode)
        assertTrue(result.stdout.contains('user="load_user"'))
        assertFalse(new File(result.stdout.readLines()[0]).exists())
    }

    @Test
    void removesCredentialsWhenProcessCannotStart() {
        def directory = new File(System.getProperty('java.io.tmpdir'))
        def credentialFiles = { directory.listFiles().findAll { it.name.startsWith('doris-mysql-') }.collect { it.name }.toSet() }
        def before = credentialFiles()
        assertThrows(IOException) {
            MySqlClient.execute('root', 'fake-secret', [], '', null, tempDir.resolve('missing-client').toString())
        }
        assertEquals(before, credentialFiles())
    }
}
