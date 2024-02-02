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

suite("test_javaudf_mv") {
    def jarPath = """${context.file.parent}/jars/java-udf-case-jar-with-dependencies.jar"""
    log.info("Jar path: ${jarPath}".toString())
    File path = new File(jarPath)
    if (!path.exists()) {
        throw new IllegalStateException("""${jarPath} doesn't exist! """)
    }

    try_sql("DROP FUNCTION IF EXISTS java_udf_mv_test(int);")

    sql """ CREATE FUNCTION java_udf_mv_test(int) RETURNS int PROPERTIES (
        "file"="file://${jarPath}",
        "symbol"="org.apache.doris.udf.IntTest",
        "type"="JAVA_UDF"
    ); """

    
}
