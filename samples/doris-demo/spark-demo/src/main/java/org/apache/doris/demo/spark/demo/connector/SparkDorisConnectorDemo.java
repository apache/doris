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

package org.apache.doris.demo.spark.demo.connector;


import org.apache.doris.demo.spark.constant.SparkToDorisConstants;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This class is a java demo for doris spark connector,
 * and provides way to read doris tables build DF using spark doris connector .
 * before you run this class, you need to build doris-spark,
 * and put the doris-spark jar file in your maven repository
 */

public class SparkDorisConnectorDemo {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkDorisConnectorDemo")
                .setMaster("local[1]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> dorisSparkDF = sparkSession.read().format("doris")
                .option("doris.table.identifier", SparkToDorisConstants.DORIS_DBNAME.concat(".").concat(SparkToDorisConstants.DORIS_TABLE_NAME))
                .option("doris.fenodes", SparkToDorisConstants.DORIS_FE_NODES)
                .option("user", SparkToDorisConstants.DORIS_USER)
                .option("password", SparkToDorisConstants.DORIS_PASSWORD)
                .load();

        dorisSparkDF.groupBy("name").count().show();

        sparkSession.close();
    }
}
