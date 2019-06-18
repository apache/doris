/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.palo

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.net.URL
import java.sql.{PreparedStatement, ResultSet}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import org.mockito.stubbing.Answer
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Only for Streaming
 */
class PaloSinkSuite extends StreamTest with SharedSQLContext {  
  import testImplicits._   
    
  test("create PaloSink by PaloSinkProvider") {
    val provider =  new PaloSinkProvider()
    val parameters = baseParameters
    val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Append)
    assert(sink.isInstanceOf[PaloSink])
  } 

  test("throw IllegalArgumentException when not provide necessary config")  {
    val provider = new PaloSinkProvider()  
    val parameters = Map[String, String]()
    val caught = intercept[IllegalArgumentException] {
      val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Append)
    }
    assert(caught.getMessage.contains("must be specified"))
  }

  test("throw IllegalArgumentException when not provide COMPUTENODEURL of newWay")  {
    val provider = new PaloSinkProvider()  
    val parameters = Map[String, String](
      PaloConfig.USERNAME -> "demo",
      PaloConfig.PASSWORD -> "demo",
      PaloConfig.DATABASE-> "palosink",
      PaloConfig.TABLE -> "metrics_table")
    val caught = intercept[IllegalArgumentException] {
      val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Append)
    }
    assert(caught.getMessage.contains(s"${PaloConfig.COMPUTENODEURL} must be specified"))
  }

  test("throw IllegalArgumentException when not provide MYSQLURL of newWay")  {
    val provider = new PaloSinkProvider()  
    val parameters = Map[String, String](
      PaloConfig.COMPUTENODEURL -> "http://www.compute.com:8090",
      PaloConfig.USERNAME -> "demo",
      PaloConfig.PASSWORD -> "demo",
      PaloConfig.DATABASE-> "palosink",
      PaloConfig.TABLE -> "metrics_table")
    val caught = intercept[IllegalArgumentException] {
      val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Append)
    }
    assert(caught.getMessage.contains(s"${PaloConfig.MYSQLURL} must be specified"))
  }

  test("throw IllegalArgumentException when Outputmode is Complete")  {
    val provider = new PaloSinkProvider()  
    val parameters = baseParameters
    val caught = intercept[IllegalArgumentException] {
      val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Complete)
    }
    assert(caught.getMessage.contains("Append/Update for palo sink"))
  }

  test("test Update Outputmode")  {
    val provider = new PaloSinkProvider()  
    val parameters = baseParameters
    val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Update)
    assert(sink.isInstanceOf[PaloSink])
  }

  test("check PaloSink if batchId less than latestBatchId") {
    val provider =  new PaloSinkProvider()
    val parameters = baseParameters
    val sink: Sink = provider.createSink(spark.sqlContext, parameters, null, OutputMode.Append) 
    sink.addBatch(-2, null)
  }

  test("test PaloClient config") {
    val parameters = baseParameters ++ 
      Map(PaloConfig.CONNECTION_MAX_RETRIES -> "10",
          PaloConfig.LOGIN_TIMEOUT_SEC -> "20", 
          PaloConfig.NETWORK_TIMEOUT_MS -> "30",
          PaloConfig.QUERY_TIMEOUT_SEC -> "40")
    val client = new PaloClient(parameters)
    assert(client.connectionMaxRetries == 10)
    assert(client.loginTimeoutSec == 20)
    assert(client.networkTimeoutMillis == 30)
    assert(client.queryTimeoutSec == 40)
    assert(client.url.contains("http://www.compute.com:9030"))
    assert(client.username.equals("demo"))
    assert(client.password.equals("demo"))
    assert(client.database.equals("palosink"))
    assert(client.table.equals("metrics_table"))
  }
  
  test("check PaloDfsLoadTask parameter") {
    // 1. check required parameters
    val dataDir = "tmpDir"
     
    // 1) miss loadcmd
    var caught = intercept[IllegalArgumentException] {
       val tmpParameters = baseParameters ++
         Map("hadoop.job.ugi" -> "palo,palo123",  
             PaloConfig.PALO_DATA_DIR -> dataDir)
       val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest1", tmpParameters,
         new StructType(Array(StructField("value", StringType, true))))
    } 
    assert(caught.getMessage.contains(s"${PaloConfig.LOADCMD} must be specified"))

    // 2) miss ugi
    caught = intercept[IllegalArgumentException] {
       val tmpParameters = baseParameters ++
         Map(PaloConfig.LOADCMD -> "cmd",
             PaloConfig.PALO_DATA_DIR -> dataDir)
       val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest2", tmpParameters,
         new StructType(Array(StructField("value", StringType, true))))
    } 
    assert(caught.getMessage.contains("hadoop.job.ugi must be specified"))

    // 3) miss palo.data.dir
    caught = intercept[IllegalArgumentException] {
       val tmpParameters = baseParameters ++
         Map(PaloConfig.LOADCMD -> "cmd",
             "hadoop.job.ugi" -> "palo,palo123")
       val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest3", tmpParameters,
         new StructType(Array(StructField("value", StringType, true))))
    } 
    assert(caught.getMessage.contains("palo.data.dir must be specified"))

    // 2. check other optional parameters 
    val parameters = baseParameters ++ 
      Map(PaloConfig.LOADCMD -> "cmd",
          PaloConfig.PALO_DATA_DIR -> dataDir,
          PaloConfig.DEFAULT_RETRY_INTERVAL_MS -> "10",
          PaloConfig.QUERY_RETRY_INTERVAL_MS -> "20",
          PaloConfig.QUERY_MAX_RETRIES -> "30",
          PaloConfig.PALO_TIME_OUT -> "40",
          PaloConfig.MAX_FILTER_RATIO -> "0.2",
          PaloConfig.LOAD_DELETE_FLAG -> "true",
          "hadoop.job.ugi" -> "palo,palo123")
    val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest4", parameters,
      new StructType(Array(StructField("value", StringType, true))))
    
    assert(task.username.equals("demo"))
    assert(task.password.equals("demo"))
    assert(task.database.equals("palosink"))
    assert(task.table.equals("metrics_table"))
    assert(task.defaultRetryIntervalMs == 10)
    assert(task.queryRetryIntervalMs == 20)
    assert(task.queryMaxRetries == 30)
    assert(task.paloTimeout == 40)
    assert(task.maxFilterRatio == 0.2)
    assert(task.loadDeletFlag.equals("true"))

    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.listFiles.foreach{ f =>
        f.delete 
      }
      dir.delete  
    }
  } 

  test("check parameters of PaloBulkLoadTask to access Palo") { 
    // 1. check required parameters
    val parameters = baseParameters ++ Map(
        PaloConfig.SEPARATOR -> ":",
        PaloConfig.HTTP_MAX_RETRIES -> "10",
        PaloConfig.HTTP_CONNECT_TIMEOUT_MS -> "20",
        PaloConfig.HTTP_READ_TIMEOUT_MS -> "30",
        PaloConfig.BULK_LOAD_READ_BUFFERSIZE -> "128",
        PaloConfig.HTTP_PORT -> "5678",
        PaloConfig.MAX_FILTER_RATIO -> "0.5")
    val task = new PaloBulkLoadTask( spark, 2, "bulkLoadTest", parameters,
        new StructType(Array(StructField("value", StringType, true))))
    assert(task.httpMaxRetries == 10)
    assert(task.httpConnectTimeoutMs == 20)
    assert(task.httpReadTimeoutMs == 30)
    assert(task.bulkLoadReadBufferSize == 128)
    assert(task.httpPort.equals("5678"))
    assert(task.separator.equals(":"))
    assert(task.maxFilterRatio == 0.5)

    // 2. check dist url of task
    val url = task.generateDistUrl
    assert(s"http://${parameters(PaloConfig.COMPUTENODEURL)}/"
        + s"${task.urlSuffix}" == url.toString)

    // 3. check PaloClient url
    val client = task.client 
    assert(s"${client.prefix}${parameters(PaloConfig.MYSQLURL)}/"
        + s"${parameters(PaloConfig.DATABASE)}${client.suffix}" == client.url.toString)

    val file = new File(task.label)
    if (file.exists) {
      file.delete 
    }
  }

  test("test PaloDfsLoadTask methods except execute()") {
    val dataDir= "paloDfsLoad_data"
    val parameters = baseParameters ++ 
          Map(PaloConfig.LOADCMD -> "cmd",
          "hadoop.job.ugi" -> "palo,palo123",  
          "palo.data.dir" -> dataDir)
    val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task.client = mockClient
    
    // write content to file
    assert(task.writeToFile("palo\n") == 5) 
    assert(task.writeToFile("test\n") == 5)
    task.loadFileToPaloTable
    val file = new File(s"${dataDir}/${task.label}")
    assert(file.exists())
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
    val data = new ArrayBuffer[String]()
    var line = reader.readLine()
    while (line != null) {
      data += line 
      line = reader.readLine()
    }
    assert(data(0).equals("palo"))
    assert(data(1).equals("test"))

    // delete tmp file
    task.deleteFile
    
    // close task
    task.close()

    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }

  test("test PaloDfsLoadTask execute") {
    val dataDir= "paloDfsLoad_data"
    val parameters = baseParameters ++ 
          Map(PaloConfig.LOADCMD -> "cmd",
          "hadoop.job.ugi" -> "palo,palo123",  
          "palo.data.dir" -> dataDir)
    // FileSystem create by PaloDfsLoadTask is local filesystem
    val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task.client = mockClient
     
    // runt task
    task.execute(getRows)

    // delete tmp file
    task.deleteFile

    // check whether delete file successfully
    assert(!task.fs.exists(task.dataPath))
 
    // close task
    task.close()
    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }

  test("test PaloBulkLoadTask methods except execute()") {
    val dataDir= "paloBulkLoad_data"
    val parameters = baseParameters;
    val task = new PaloBulkLoadTaskForUT(spark, 2, "bulkLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task.client = mockClient
    
    // write content to file
    assert(task.writeToFile("palo\n") == 5) 
    assert(task.writeToFile("test\n") == 5)
    task.loadFileToPaloTable
    val file = new File(s"${task.label}")
    assert(file.exists())
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)))
    val data = new ArrayBuffer[String]()
    var line = reader.readLine()
    while (line != null) {
      data += line 
      line = reader.readLine()
    }
    assert(data(0).equals("palo"))
    assert(data(1).equals("test"))

    // close task and do not delete file
    task.close()

    // create a new Task with the same file to write data
    val task2 = new PaloBulkLoadTaskForUT(spark, 2, "bulkLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task2.deleteFile
    task2.close() 
    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }
 
  test("test PaloBulkLoadTask execute") {
    val dataDir= "paloBulkLoad_data"
    val parameters = baseParameters ++ 
          Map(PaloConfig.LOADCMD -> "cmd",
          "hadoop.job.ugi" -> "palo,palo123",  
          "palo.data.dir" -> dataDir)
    // FileSystem create by PaloDfsLoadTask is local filesystem
    val task = new PaloBulkLoadTaskForUT(spark, 2, "bulkLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task.client = mockClient
     
    // runt task
    task.execute(getRows)

    // delete tmp file
    task.deleteFile
    // check whether delete file successfully
    assert(!task.file.exists)
     
    // close task
    task.close()
  
    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }
  
  test("test PaloDfsLoadTask execute empty row") {
    val dataDir= "paloDfsLoad_data"
    val parameters = baseParameters ++ 
          Map(PaloConfig.LOADCMD -> "cmd",
          PaloConfig.DELETE_FILE -> "false",
          "hadoop.job.ugi" -> "palo,palo123",
          "palo.data.dir" -> dataDir)
    // FileSystem create by PaloDfsLoadTask is local filesystem
    val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task.client = mockClient
     
    // runt task
    task.execute(List[InternalRow]().iterator)
    assert(task.writeBytesSize == 0)     

    // needDelete flag is false, can't delete file
    task.deleteFile
    assert(task.fs.exists(task.dataPath)) 
    // delete tmp file
    task.fs.delete(task.dataPath, true)
     
    // close task
    task.close()
  
    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }

  test("test PaloBulkLoadTask execute empty row") {
    val dataDir= "paloBulkLoad_data"
    val parameters = baseParameters ++ 
          Map(PaloConfig.DELETE_FILE -> "false")
    // FileSystem create by PaloDfsLoadTask is local filesystem
    val task = new PaloBulkLoadTaskForUT(spark, 2, "bulkLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))
    task.client = mockClient
     
    // runt task
    task.execute(List[InternalRow]().iterator)
    assert(task.writeBytesSize == 0)     

    // needDelete flag is false, can't delete file
    task.deleteFile
    assert(task.file.exists) 
    // delete tmp file
    task.file.delete

    // close task
    task.close()
  
    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }
 
  test("test ensureLoadEnd method in PaloWriterTask") {
    val dataDir= "paloDfsLoad_data"
    val parameters = baseParameters ++ 
          Map(PaloConfig.LOADCMD -> "cmd",
          "hadoop.job.ugi" -> "palo,palo123",  
          "palo.data.dir" -> dataDir,
          PaloConfig.QUERY_RETRY_INTERVAL_MS -> "1")
    val task = new PaloDfsLoadTask(spark, 2, "dfsLoadTest", parameters,
       new StructType(Array(StructField("value", StringType, true))))

    // 1) normal client but preLoadInfo's jobId is wrong order
    task.client = mockClient
    intercept[IllegalStateException] {
      task.ensureLoadEnd(Some(new PaloLoadInfo(2, "label", "ETL", "do elt")))
    }
    task.ensureLoadEnd(Some(new PaloLoadInfo(2, "label", "FINISHED", "finish"))) 

    // 2) client that return emtpy set for the first query
    task.client = mockClientWithEmptySetFirstQuery
    intercept[PaloIllegalStateException] {
      task.ensureLoadEnd(None)
    }

    // delete tmp file
    task.deleteFile
    task.close()

    // delete palo.data.dir
    val dir = new File(dataDir)
    if (dir.exists) {
      dir.delete  
    }
  }

  test("write data with invalid type") {
    val input = MemoryStream[Int] 
    var writer: StreamingQuery = null
    var caught: Throwable = null;
    try {
      caught = intercept[StreamingQueryException] {
        writer = createPaloWriter(input.toDF(), 
          withOptions = baseParameters,
          withOutputMode = Some(OutputMode.Append))    
        input.addData(1,2,3,4,5)
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(caught.getMessage.contains("must have only one StringType StructField"))
  }
  
  test("test generateLabel") {
    val task = new PaloBulkLoadTaskForUT(spark, 3, "TestLabel", baseParameters,
       new StructType(Array(StructField("value", StringType, true))))
    val suffix = "_3_0"
    val label1 = "/hello"
    assert(task.generateLabel(label1) == "hello" + suffix)
  
    val label2 = "label"
    assert(task.generateLabel(label2) == "label" + suffix) 
  
    val label3 = "hdfs://my-label/test.go//"
    assert(task.generateLabel(label3) == "test_go" + suffix)
  
    val builder = new StringBuilder()
    for (i <- 0 to 128) {
      builder.append("a") 
    }
    val label4 = "prefix" + builder.toString
    val all = label4 + suffix
    val last128 = all.substring(all.length - 128, all.length)
    val parseLabel = task.generateLabel(label4)
    assert(label4.length > 128)
    assert(parseLabel.length == 128 && parseLabel == last128)
  }

  private def createPaloWriter(  
      input: DataFrame, 
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]()): StreamingQuery = { 
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      stream = df.writeStream
        .format("palo")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }

  // common client
  private def mockClient: PaloClient = {
    val client = mock(classOf[PaloClient])
    doNothing.when(client).start()
    doNothing.when(client).close()

    val preparedStatement = mock(classOf[PreparedStatement])
    val resultSet = mock(classOf[ResultSet])
    when(resultSet.next()).thenReturn(true).thenReturn(true)
    when(resultSet.getLong("JobId")).thenReturn(1).thenReturn(2).thenReturn(3)
    when(resultSet.getString("Label")).thenReturn("label")
    when(resultSet.getString("State")).thenReturn("LOAIDNG").thenReturn("FINISHED")
    when(resultSet.getString("ErrorMsg")).thenReturn("no error msg")

    when(client.getPreparedStatement(anyString())).thenReturn(preparedStatement)
    when(preparedStatement.execute()).thenReturn(true)
    when(preparedStatement.executeQuery()).thenReturn(resultSet)

    client
  }
 
  // this client will return empty set for the first query
  // and the State is from "LOADING" to "CANCELLED" 
  private def mockClientWithEmptySetFirstQuery: PaloClient = {
    val client = mock(classOf[PaloClient])
    doNothing.when(client).start()
    doNothing.when(client).close()

    val preparedStatement = mock(classOf[PreparedStatement])
    val resultSet = mock(classOf[ResultSet])
    when(resultSet.next()).thenReturn(false).thenReturn(true)
    when(resultSet.getLong("JobId")).thenReturn(1).thenReturn(2)
    when(resultSet.getString("Label")).thenReturn("label")
    when(resultSet.getString("State")).thenReturn("LOAIDNG").thenReturn("CANCELLED")
    when(resultSet.getString("ErrorMsg")).thenReturn("cancel msg")

    when(client.getPreparedStatement(anyString())).thenReturn(preparedStatement)
    when(preparedStatement.execute()).thenReturn(true)
    when(preparedStatement.executeQuery()).thenReturn(resultSet)

    client
  }
 
  private def getRows(): Iterator[InternalRow] = {
    val list = List(new GenericInternalRow(Array[Any](UTF8String.fromString("123"))),
      new GenericInternalRow(Array[Any](UTF8String.fromString("palo"))),
      new GenericInternalRow(Array[Any](UTF8String.fromString("sink"))))
    list.iterator
  }

  private def baseParameters: Map[String, String] = {
    Map(PaloConfig.COMPUTENODEURL -> "http://www.compute.com:8040", 
      PaloConfig.MYSQLURL -> "http://www.compute.com:9030", 
      PaloConfig.USERNAME -> "demo",
      PaloConfig.PASSWORD -> "demo",
      PaloConfig.DATABASE-> "palosink",
      PaloConfig.TABLE -> "metrics_table")
  }
 
  private class PaloBulkLoadTaskForUT(
      sparkSession: SparkSession,
      batchId: Long,
      checkpointRoot: String,
      parameters: Map[String, String],
      schema: StructType) extends PaloBulkLoadTask(sparkSession, 
          batchId, checkpointRoot, parameters, schema) {
    override def loadByHttp(url: URL) {
      // do nothing
    }
  } 
}
