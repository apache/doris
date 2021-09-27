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

import org.apache.doris.spark.DorisStreamLoad
import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, Filter, RelationProvider}

import java.io.IOException
import java.util
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.util.control.Breaks

private[sql] class DorisSourceProvider extends DataSourceRegister with RelationProvider with CreatableRelationProvider with StreamWriteSupport with Logging {
  override def shortName(): String = "doris"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    new DorisRelation(sqlContext, Utils.params(parameters, log))
  }


  /**
   * df.save
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode, parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {

    val sparkSettings = new SparkSettings(sqlContext.sparkContext.getConf)
    sparkSettings.merge(Utils.params(parameters, log).asJava)
    // init stream loader
    val dorisStreamLoader = new DorisStreamLoad(sparkSettings)

    val maxRowCount = sparkSettings.getIntegerProperty(ConfigurationOptions.DORIS_BATCH_SIZE, ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT)
    val maxRetryTimes = sparkSettings.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_RETRIES, ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT)

    data.rdd.foreachPartition(partition => {
      val rowsBuffer: util.List[util.List[Object]] = new util.ArrayList[util.List[Object]](maxRowCount)
      partition.foreach(row => {
        val line: util.List[Object] = new util.ArrayList[Object]()
        for (i <- 0 until row.size) {
          val field = row.get(i)
          line.add(field.asInstanceOf[AnyRef])
        }
        rowsBuffer.add(line)
        if (rowsBuffer.size > maxRowCount) {
          flush
        }
      })
      // flush buffer
      if (!rowsBuffer.isEmpty) {
        flush
      }

      /**
       * flush data to Doris and do retry when flush error
       *
       */
      def flush = {
        val loop = new Breaks
        loop.breakable {

          for (i <- 1 to maxRetryTimes) {
            try {
              dorisStreamLoader.load(rowsBuffer)
              rowsBuffer.clear()
              loop.break()
            }
            catch {
              case e: Exception =>
                try {
                  Thread.sleep(1000 * i)
                  dorisStreamLoader.load(rowsBuffer)
                  rowsBuffer.clear()
                } catch {
                  case ex: InterruptedException =>
                    Thread.currentThread.interrupt()
                    throw new IOException("unable to flush; interrupted while doing another attempt", e)
                }
            }
          }
        }

      }

    })
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException

      override def schema: StructType = unsupportedException

      override def needConversion: Boolean = unsupportedException

      override def sizeInBytes: Long = unsupportedException

      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException

      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from doris write operation is not usable.")
    }
  }

  override def createStreamWriter(queryId: String, structType: StructType, outputMode: OutputMode, dataSourceOptions: DataSourceOptions): StreamWriter = {
    val sparkSettings = new SparkSettings(new SparkConf())
    sparkSettings.merge(Utils.params(dataSourceOptions.asMap().toMap, log).asJava)
    new DorisStreamWriter(sparkSettings)
  }
}
