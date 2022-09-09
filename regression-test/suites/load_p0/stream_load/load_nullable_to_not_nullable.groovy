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

suite("load_nullable_to_not_nullable") {
    def tableName = "load_nullable_to_not_nullable"
    def dbName = "test_query_db"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE $dbName"
    sql "DROP TABLE IF EXISTS ${tableName} "
    sql """
        CREATE TABLE `${tableName}` (
            k1 int(32) NOT NULL,
            k2 smallint NOT NULL,
            k3 int NOT NULL,
            k4 bigint NOT NULL,
            k5 decimal(9, 3) NOT NULL,
            k6 char(5) NOT NULL,
            k10 date NOT NULL,
            k11 datetime NOT NULL,
            k7 varchar(20) NOT NULL,
            k8 double max NOT NULL,
            k9 float sum NOT NULL )
        AGGREGATE KEY(k1,k2,k3,k4,k5,k6,k10,k11,k7)
        PARTITION BY RANGE(k2) (
            PARTITION partition_a VALUES LESS THAN MAXVALUE
        )
        DISTRIBUTED BY HASH(k1, k2, k5)
        BUCKETS 3
        PROPERTIES ( "replication_allocation" = "tag.location.default: 1");
        """

    StringBuilder commandBuilder = new StringBuilder()
    commandBuilder.append("""curl -v --location-trusted -u ${context.config.feHttpUser}:${context.config.feHttpPassword}""")
    commandBuilder.append(""" -H columns:col,k1=year(col),k2=month(col),k3=month(col),k4=day(col),k5=7.7,k6='a',k10=date(col),k11=FROM_UNIXTIME(2019,'%Y-%m-%dT%H:%i:%s'),k7='k7',k8=month(col),k9=day(col) -T ${context.file.parent}/data/test_time.data http://${context.config.feHttpAddress}/api/""" + dbName + "/" + tableName + "/_stream_load")
    String command = commandBuilder.toString()
    def process = command.execute()
    int code = process.waitFor()
    String err = IOGroovyMethods.getText(new BufferedReader(new InputStreamReader(process.getErrorStream())));
    String out = process.getText()
    logger.info("Run command: command=" + command + ",code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    qt_sql " SELECT * FROM ${tableName} "
    sql "DROP TABLE ${tableName} "
}

