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
suite("test_nereids_encrypt_test") {
    def dbName="test_nereids_encrypt_test_db"
    def encryptkeyName="test_nereids_encrypt_test_key"
    sql """ create database IF NOT EXISTS ${dbName}; """
    sql """ use ${dbName}; """
    checkNereidsExecute("drop encryptkey if exists ${encryptkeyName}")    
    checkNereidsExecute("""CREATE ENCRYPTKEY ${encryptkeyName} AS "ABCD123456789";""")
    checkNereidsExecute("SHOW ENCRYPTKEYS FROM ${dbName}")
    qt_check_encrypt_1("SHOW ENCRYPTKEYS FROM ${dbName}")
    checkNereidsExecute("drop encryptkey ${encryptkeyName}")
    checkNereidsExecute("SHOW ENCRYPTKEYS FROM ${dbName}")
    qt_check_encrypt_2("SHOW ENCRYPTKEYS FROM ${dbName}")    
    checkNereidsExecute("drop encryptkey if exists ${encryptkeyName}")
    qt_check_encrypt_3("SHOW ENCRYPTKEYS FROM ${dbName}")        
}
