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
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Ignore;
import org.junit.Test

// This test need real connect info to run.
// Set the connect info before comment out this @Ignore
@Ignore
class TestSparkConnector {
  val dorisFeNodes = "your_fe_host:8030"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_tbl"

  val kafkaServers = ""
  val kafkaTopics = ""

  @Test
  def rddReadTest(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    import org.apache.doris.spark._
    val dorisSparkRDD = sc.dorisRDD(
      tableIdentifier = Some(dorisTable),
      cfg = Some(Map(
        "doris.fenodes" -> dorisFeNodes,
        "doris.request.auth.user" -> dorisUser,
        "doris.request.auth.password" -> dorisPwd
      ))
    )
    dorisSparkRDD.map(println(_)).count()
    sc.stop()
  }

  @Test
  def dataframeWriteTest(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val df = session.createDataFrame(Seq(
      ("zhangsan", "m"),
      ("lisi", "f"),
      ("wangwu", "m")
    ))
    df.write
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      //specify your field
      .option("doris.write.field", "name,gender")
      .option("sink.batch.size",2)
      .option("sink.max-retries",2)
      .save()
    session.stop()
  }

  @Test
  def dataframeReadTest(): Unit = {
    val session = SparkSession.builder().master("local[*]").getOrCreate()
    val dorisSparkDF = session.read
      .format("doris")
      .option("doris.fenodes", dorisFeNodes)
      .option("doris.table.identifier", dorisTable)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .load()

    dorisSparkDF.show()
    session.stop()
  }


  @Test
  def structuredStreamingWriteTest(): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    val df = spark.readStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("startingOffsets", "latest")
      .option("subscribe", kafkaTopics)
      .format("kafka")
      .option("failOnDataLoss", false)
      .load()

    df.selectExpr("CAST(timestamp AS STRING)", "CAST(partition as STRING)")
      .writeStream
      .format("doris")
      .option("checkpointLocation", "/tmp/test")
      .option("doris.table.identifier", dorisTable)
      .option("doris.fenodes", dorisFeNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      .option("sink.batch.size",2)
      .option("sink.max-retries",2)
      .start().awaitTermination()
  }
}

