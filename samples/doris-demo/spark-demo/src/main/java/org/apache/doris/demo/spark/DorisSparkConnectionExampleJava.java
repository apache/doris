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

package org.apache.doris.demo.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class is a java demo for doris spark connector,
 * and provides three ways to read doris table using spark.
 * before you run this class, you need to build doris-spark,
 * and put the doris-spark jar file in your maven repository
 */
public class DorisSparkConnectionExampleJava {

    private static final String DORIS_DB = "demo";

    private static final String DORIS_TABLE = "example_table";

    private static final String DORIS_FE_IP = "your doris fe ip";

    private static final String DORIS_FE_HTTP_PORT = "8030";

    private static final String DORIS_FE_QUERY_PORT = "9030";

    private static final String DORIS_USER = "your doris user";

    private static final String DORIS_PASSWORD = "your doris password";

    public static void main(String[] args) {
        SparkSession sc = SparkSession.builder().master("local[*]").appName("test").getOrCreate();
        readWithDataFrame(sc);
        //readWithSparkSql(sc);
        //readWithJdbc(sc);
    }

    /**
     * read doris table Using DataFrame
     *
     * @param sc SparkSession
     */
    private static void readWithDataFrame(SparkSession sc) {
        Dataset<Row> df = sc.read().format("doris")
                .option("doris.table.identifier", String.format("%s.%s", DORIS_DB, DORIS_TABLE))
                .option("doris.fenodes", String.format("%s:%s", DORIS_FE_IP, DORIS_FE_HTTP_PORT))
                .option("user", DORIS_USER)
                .option("password", DORIS_PASSWORD)
                .load();
        df.show(5);
    }

    /**
     * read doris table Using Spark Sql
     *
     * @param sc SparkSession
     */
    private static void readWithSparkSql(SparkSession sc) {
        sc.sql("CREATE TEMPORARY VIEW spark_doris " +
                "USING doris " +
                "OPTIONS( " +
                "  \"table.identifier\"=\"" + DORIS_DB + "." + DORIS_TABLE + "\", " +
                "  \"fenodes\"=\"" + DORIS_FE_IP + ":" + DORIS_FE_HTTP_PORT + "\", " +
                "  \"user\"=\"" + DORIS_USER + "\", " +
                "  \"password\"=\"" + DORIS_PASSWORD + "\" " +
                ")");
        sc.sql("select * from spark_doris").show(5);
    }

    /**
     * read doris table Using jdbc
     *
     * @param sc SparkSession
     */
    private static void readWithJdbc(SparkSession sc) {
        Dataset<Row> df = sc.read().format("jdbc")
                .option("url", String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8", DORIS_FE_IP, DORIS_FE_QUERY_PORT, DORIS_DB))
                .option("dbtable", DORIS_TABLE)
                .option("user", DORIS_USER)
                .option("password", DORIS_PASSWORD)
                .load();
        df.show(5);
    }
}