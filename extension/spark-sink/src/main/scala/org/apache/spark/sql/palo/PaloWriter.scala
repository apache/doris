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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.streaming.IncrementalExecution
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils


/**
 * The [[PaloWriter]] class is used to write data from a batch query
 * or structured streaming query, given by a [[QueryExecution]], to Palo.
 */
private[palo] object PaloWriter extends Logging {
  override def toString: String = "PaloWriter"

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      checkpointRoot: String,
      batchId: Long,
      parameters: Map[String, String]): Unit = {
    val schema = queryExecution.analyzed.schema

    // check schema
    assert(schema.length == 1 && schema.fields(0).dataType.isInstanceOf[StringType],
      "the dataFrame to sink's schema must have only one StringType StructField")

    import org.apache.spark.sql.palo.LoadWay._
    // choose Palo loading way, contains three ways
    //   1) LOAD, for big files on hdfs
    //   2) BULK LOAD, for files which size small than 1GB
    //   3) TODO: support Streaming LOAD
    //
    // if parameters contains key "loadway", use the specified loadway
    // otherwise use the default way "LOAD"
    val loadWay: LoadWay = parameters.getOrElse(PaloConfig.LOADWAY, "BULK_LOAD") match {
       case "BULK_LOAD" => BULK_LOAD
       case "LOAD" => LOAD
       case way =>
         logWarning(s"loadway must be LOAD or BULK_LOAD, should not be $way, will use LOAD way")
         LOAD
    }

    if (loadWay == LOAD) {
      PaloSinkProvider.check(PaloConfig.BROKER_NAME, parameters)
    }
    logInfo(s"use $loadWay to sink data to palo")

    SQLExecution.withNewExecutionId(sparkSession, queryExecution) {
      queryExecution.toRdd.foreachPartition { iter =>
        val writeTask: PaloWriteTask = loadWay match {
          case BULK_LOAD =>
            new PaloBulkLoadTask(sparkSession, batchId, checkpointRoot, parameters, schema)
          case LOAD =>
            new PaloDfsLoadTask(sparkSession, batchId, checkpointRoot, parameters, schema)
        }
        Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
          finallyBlock = writeTask.close())
      }
    }
  }
} // end class of PaloWriter

object LoadWay extends Enumeration {
  type LoadWay = Value

  val LOAD = Value
  val BULK_LOAD = Value
}
