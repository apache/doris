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

package org.apache.doris.spark.sql

import org.apache.spark.{SparkConf, SparkContext}



object SparkDorisConnector {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("SparkDorisConnector").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        sc.setLogLevel("DEBUG")
        import org.apache.doris.spark._
        val dorisSparkRDD = sc.dorisRDD(
            tableIdentifier = Some("db.table1"),
            cfg = Some(Map(
                "doris.fenodes" -> "feip:8030",
                "doris.request.auth.user" -> "root",
                "doris.request.auth.password" -> ""
            ))
        )

        dorisSparkRDD.map(println(_)).count()
        sc.stop()
    }

}
