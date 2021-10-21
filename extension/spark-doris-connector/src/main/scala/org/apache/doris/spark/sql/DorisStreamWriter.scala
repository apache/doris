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

import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}
import org.apache.doris.spark.{CachedDorisStreamLoadClient, DorisStreamLoad}
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}

import java.io.IOException
import java.util
import scala.util.control.Breaks

/**
 * A [[StreamWriter]] for Apache Doris streaming writing.
 *
 * @param settings params for writing doris table
 */
class DorisStreamWriter(settings: SparkSettings) extends StreamWriter {
  override def createWriterFactory(): DorisStreamWriterFactory = DorisStreamWriterFactory(settings)

  override def commit(l: Long, writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(l: Long, writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

}

/**
 * A [[DataWriterFactory]] for Apache Doris streaming writing. Will be serialized and sent to executors to generate
 * the per-task data writers.
 *
 * @param settings params for writing doris table
 */
case class DorisStreamWriterFactory(settings: SparkSettings) extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new DorisStreamDataWriter(settings)
  }
}

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but we
 * don't need to really send one.
 */
case object DorisWriterCommitMessage extends WriterCommitMessage

/**
 * A [[DataWriter]] for Apache Doris streaming writing. One data writer will be created in each partition to
 * process incoming rows.
 *
 * @param settings params for writing doris table
 */
class DorisStreamDataWriter(settings: SparkSettings) extends DataWriter[Row] {
  val maxRowCount: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_BATCH_SIZE, ConfigurationOptions.DORIS_BATCH_SIZE_DEFAULT)
  val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.DORIS_REQUEST_RETRIES, ConfigurationOptions.DORIS_REQUEST_RETRIES_DEFAULT)
  val dorisStreamLoader: DorisStreamLoad = CachedDorisStreamLoadClient.getOrCreate(settings)
  val rowsBuffer: util.List[util.List[Object]] = new util.ArrayList[util.List[Object]](maxRowCount)

  override def write(row: Row): Unit = {
    val line: util.List[Object] = new util.ArrayList[Object]()
    for (i <- 0 until row.size) {
      val field = row.get(i)
      line.add(field.asInstanceOf[AnyRef])
    }
    if (!line.isEmpty) {
      rowsBuffer.add(line)
    }
    if (rowsBuffer.size >= maxRowCount) {
      // commit when buffer is full
      commit()
    }
  }

  override def commit(): WriterCommitMessage = {
    // we don't commit request until rows-buffer received some rows
    val loop = new Breaks
    loop.breakable {
      for (i <- 1 to maxRetryTimes) {
        try {
          if (!rowsBuffer.isEmpty) {
            dorisStreamLoader.load(rowsBuffer)
          }
          rowsBuffer.clear()
          loop.break()
        }
        catch {
          case e: Exception =>
            try {
              Thread.sleep(1000 * i)
              if (!rowsBuffer.isEmpty) {
                dorisStreamLoader.load(rowsBuffer)
              }
              rowsBuffer.clear()
            } catch {
              case ex: InterruptedException =>
                Thread.currentThread.interrupt()
                throw new IOException("unable to flush; interrupted while doing another attempt", e)
            }
        }
      }
    }
    DorisWriterCommitMessage
  }

  override def abort(): Unit = {
  }
}