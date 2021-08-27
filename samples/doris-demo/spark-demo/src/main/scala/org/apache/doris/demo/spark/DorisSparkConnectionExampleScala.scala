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

package org.apache.doris.demo.spark

/**
 * This class is a scala demo for doris spark connector,
 * and provides four ways to read doris tables using spark.
 * before you run this class, you need to build doris-spark,
 * and put the doris-spark jar file in your maven repository
 */

object DorisSparkConnectionExampleScala {

    val DORIS_DB = "demo"

    val DORIS_TABLE = "example_table"

    val DORIS_FE_IP = "your doris fe ip"

    val DORIS_FE_HTTP_PORT = "8030"

    val DORIS_FE_QUERY_PORT = "9030"

    val DORIS_USER = "your doris user"

    val DORIS_PASSWORD = "your doris password"

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("test").setMaster("local[*]")
        // if you want to run readWithRdd(sparkConf), please comment this line
        // val sc = SparkSession.builder().config(sparkConf).getOrCreate()
        readWithRdd(sparkConf)
        // readWithDataFrame(sc)
        // readWithSql(sc)
        // readWithJdbc(sc)
    }

    /**
     * read doris table Using Spark Rdd
     */
    def readWithRdd(sparkConf: SparkConf): Unit = {
        val scf = new SparkContextFunctions(new SparkContext(sparkConf))
        val rdd = scf.dorisRDD(
            tableIdentifier = Some(s"$DORIS_DB.$DORIS_TABLE"),
            cfg = Some(Map(
                "doris.fenodes" -> s"$DORIS_FE_IP:$DORIS_FE_HTTP_PORT",
                "doris.request.auth.user" -> DORIS_USER,
                "doris.request.auth.password" -> DORIS_PASSWORD
            ))
        )
        val resultArr = rdd.collect()
        println(resultArr.mkString)
    }

    /**
     * read doris table Using DataFrame
     *
     * @param sc SparkSession
     */
    def readWithDataFrame(sc: SparkSession): Unit = {
        val df = sc.read.format("doris")
            .option("doris.table.identifier", s"$DORIS_DB.$DORIS_TABLE")
            .option("doris.fenodes", s"$DORIS_FE_IP:$DORIS_FE_HTTP_PORT")
            .option("user", DORIS_USER)
            .option("password", DORIS_PASSWORD)
            .load()
        df.show(5)
    }

    /**
     * read doris table Using Spark Sql
     *
     * @param sc SparkSession
     */
    def readWithSql(sc: SparkSession): Unit = {
        sc.sql("CREATE TEMPORARY VIEW spark_doris\n" +
            "USING doris " +
            "OPTIONS( " +
            "  \"table.identifier\"=\"" + DORIS_DB + "." + DORIS_TABLE + "\", " +
            "  \"fenodes\"=\"" + DORIS_FE_IP + ":" + DORIS_FE_HTTP_PORT + "\", " +
            "  \"user\"=\"" + DORIS_USER + "\", " +
            "  \"password\"=\"" + DORIS_PASSWORD + "\" " +
            ")")
        sc.sql("select * from spark_doris").show(5)
    }

    /**
     * read doris table Using jdbc
     *
     * @param sc SparkSession
     */
    def readWithJdbc(sc: SparkSession): Unit = {
        val df = sc.read.format("jdbc")
            .option("url", s"jdbc:mysql://$DORIS_FE_IP:$DORIS_FE_QUERY_PORT/$DORIS_DB?useUnicode=true&characterEncoding=utf-8")
            .option("dbtable", DORIS_TABLE)
            .option("user", DORIS_USER)
            .option("password", DORIS_PASSWORD)
            .load()
        df.show(5)
    }
}