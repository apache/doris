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

import org.apache.spark.sql.SparkSession

object TestStreamSinkDoris {
  val kafkaServers = ""
  val kafkaTopics = ""
  val dorisFeNodes = "your_doris_host_port"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_tbl"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local")
      .getOrCreate()

    val dataFrame = sparkSession.readStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("startingOffsets", "latest")
      .option("subscribe", kafkaTopics)
      .format("kafka")
      .option("failOnDataLoss", false)
      .load()

    dataFrame.selectExpr("CAST(timestamp AS STRING)", "CAST(partition as STRING)")
      .writeStream
      .format("doris")
      .option("checkpointLocation", "/tmp/test")
      .option("doris.table.identifier", dorisTable)
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .start().awaitTermination()
  }
}
