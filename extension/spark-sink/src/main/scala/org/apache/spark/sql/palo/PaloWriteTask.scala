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

import java.sql.{ResultSet, SQLException, SQLTimeoutException}

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * A simple trait for writing out data in a single Spark task, without any concerns about how
 * to commit or abort tasks. Exceptions thrown by the implementation of this class will
 * automatically trigger task aborts.
 *
 * @param sparkSession
 * @param batchId  id of current batch
 * @param checkpointRoot  the root checkpoint path, is also the parent directory of offsetLog
 * @param parameters  user specified parameters
 * @param schema  data schema that will write to palo, now only support one STRING_TYPE field
 */
private[palo] abstract class PaloWriteTask (
    sparkSession: SparkSession,
    batchId: Long,
    checkpointRoot: String,
    parameters: Map[String, String],
    schema: StructType) extends Logging {
  private[palo] val username: String = parameters(PaloConfig.USERNAME)
  private[palo] val password: String = parameters(PaloConfig.PASSWORD)
  private[palo] val database: String = parameters(PaloConfig.DATABASE)
  private[palo] val table: String = parameters(PaloConfig.TABLE)
  private[palo] val defaultRetryIntervalMs =
    parameters.getOrElse(PaloConfig.DEFAULT_RETRY_INTERVAL_MS, "10000").toInt
  private[palo] val queryRetryIntervalMs =
    parameters.getOrElse(PaloConfig.QUERY_RETRY_INTERVAL_MS, s"$defaultRetryIntervalMs").toInt
  private[palo] val queryMaxRetries =
    parameters.getOrElse(PaloConfig.QUERY_MAX_RETRIES, "3").toInt
  private[palo] val fieldIndex = schema.length - 1

  // For the convenience of Unit Test, use "var" instead of "val"
  // Palo guarantees the same batch of data with specified label can be imported
  // only once for one database. That is to say the 'label' like a primary key of
  // RDBMS(Relational Database Managerment System)
  private[palo] val label = generateLabel(checkpointRoot)
  private[palo] var client: PaloClient = new PaloClient(parameters)
  private[palo] var writeBytesSize: Long = 0L

  // indicate whether delete tmp file when task finished, default true
  private[palo] var needDelete =
    parameters.getOrElse(PaloConfig.DELETE_FILE, "true").toBoolean

  /**
   * Label of Palo must match the regex: ^[-_A-Za-z0-9]{1,128}$
   * so we replace all the invalid chars to '_' and for unified format
   * we also replace '-' to '_'. If the label size larger than 128,
   * keep only 128 characters of the tail
   */
  private[palo] def generateLabel(str: String): String = {
    assert(str != null && str.length >= 1)
    val tmp = str match {
      case s if (s(0) == '/') => s.substring(1, s.length)
      case s if (s.length >= 7 && s.substring(0, 7) == "hdfs://") =>
        val index = s.indexOf("/", 7)
        s.substring(index + 1, s.length)
      case _ => str
    }

    val replace = tmp.replaceAll("[-|/|:|.]", "_") +
        s"_${batchId}_${TaskContext.getPartitionId}"

    // shrink multiple underscores to one
    // e.g: label___test => label_test
    val builder = new StringBuilder
    for (index <- 0 to replace.length - 1) {
      if (index == 0) {
        builder += replace(0)
      } else if (replace(index - 1) != '_' || replace(index) != '_') {
        builder += replace(index)
      }
    }

    val resultStr = builder.toString
    val length = resultStr.length
    if (length > 128) {
      logWarning("palo label size larger than 128!, we truncate it!")
      resultStr.substring(length - 128, length)
    } else {
      resultStr
    }
  }

  /**
   * ensure the data load to palo complete, whether success or fail
   */
  private[palo] def ensureLoadEnd(preLoadInfo: Option[PaloLoadInfo]) {
    // get the jobId of preLoadInfo if exists
    val preJobId = preLoadInfo match {
      case Some(info) => Some(info.jobId)
      case None => None
    }

    var continue = true
    while (continue) {
      queryPaloLatestLoadInfo match {
        case Some(info) =>
          if (!preJobId.isEmpty && (preJobId.get > info.jobId)) {
            throw new IllegalStateException(s"the label:${label}" +
                s"pre jobId:${preJobId} should less than current qureied jobId:${info.jobId}")
          }

          if (!preJobId.isEmpty && (preJobId.get == info.jobId)) {
            // In the case: new Job has not started yet after load request, so the query return
            // the pre job info again. Retry until get new jobId > preJobId
            logWarning(s"pre jobId:${preJobId} is equal with current queried jobId:${info.jobId} " +
                s"new job has not stared yet, wait a will")
            doWait(queryRetryIntervalMs)
          } else {
            info.state match {
              case "FINISHED" =>
                continue = false
                logInfo(s"label: ${label}, load info: ${info.toString}")
              case "CANCELLED" =>
                continue = false
                logError(s"label: ${label} load failed: ${info.toString}")
                throw new PaloIllegalStateException(
                    s"label: ${label} load is cancelled: ${info.toString}")
              case _ =>
                // query again
                logInfo(s"label: ${label} load info: ${info.toString}, query again")
                doWait(queryRetryIntervalMs)
            }
          }
        case None =>
          // query again
          logInfo(s"LOAD Info of label:${label} is empty, query again")
          doWait(queryRetryIntervalMs)
      }
    } // end while loop
  }

  /**
   * query latest load info with specified label from Palo to
   * check the state of loading phase
   */
  private[palo] def queryPaloLatestLoadInfo(): Option[PaloLoadInfo] = {
    // return load info for palo
    val querySql = s"SHOW LOAD FROM ${database} WHERE" +
      " label=\"" + label + "\" ORDER BY JobId DESC LIMIT 1"
    var retryTimes = 0
    var loadInfo: Option[PaloLoadInfo] = None
    while(retryTimes <= queryMaxRetries) {
      try {
        val resultSet: ResultSet = client.getPreparedStatement(querySql).executeQuery()
        if (resultSet.next) {
          loadInfo = Some(PaloLoadInfo(resultSet.getLong("JobId"), resultSet.getString("Label"),
            resultSet.getString("State"), resultSet.getString("ErrorMsg")))
        }
        retryTimes = queryMaxRetries + 1
      } catch {
        case e: SQLTimeoutException =>
          retryTimes += 1
          if (retryTimes == queryMaxRetries + 1) {
            logWarning(s"after retry ${queryMaxRetries}, throw exception ${e.getMessage}")
          } else {
            logWarning(s"query palo load state failed for timeout: ${e.getMessage}" +
              s", retry $retryTimes time")
            doWait()
          }
        case e: SQLException =>
          logError(s"query palo load state failed: ${e.getMessage}")
          e.printStackTrace
          throw e
      }
    }
    loadInfo
  }

  private[palo] def doWait(retryTime: Long = defaultRetryIntervalMs) {
    try {
      logInfo(s"waiting for ${retryTime}ms")
      Thread.sleep(retryTime)
    } catch {
      case e : Throwable => logWarning(e.getMessage)
    }
  }

  def execute(iterator: Iterator[InternalRow]): Unit = {
    // start PaloClient
    client.start

    // write content to tmp file
    iterator.foreach { currentRow =>
      val content = currentRow.getUTF8String(fieldIndex).toString
      writeBytesSize += writeToFile(content)
    }

    // only load not empty file
    if(writeBytesSize > 0) {
      logInfo(s"file ${label} size : ${writeBytesSize}")

      // Avoid the same label to load multiple times when failover, because the
      // data of the label may be already "FINISHED" before task restart
      val preLoadInfo: Option[PaloLoadInfo] = queryPaloLatestLoadInfo
      if (preLoadInfo.isEmpty || !preLoadInfo.get.state.equals("FINISHED")) {
        loadFileToPaloTable
        // make sure the file load to Palo complete
        ensureLoadEnd(preLoadInfo)
      } else {
        logWarning(s"label: ${preLoadInfo.get.label} has been load Finished!")
      }
    } else {
      logInfo(s"file ${label} is empty")
    }
  }

  /**
   * close resources
   */
  def close(): Unit = {
    client.close
    // delete file
    deleteFile
  }

  /**
   * write string to File
   */
  def writeToFile(content: String): Long

  /**
   * load file to Palo table
   */
  def loadFileToPaloTable()

  /**
   * delete loaded file, if the needDelete flag is true
   */
  def deleteFile()
} // end class of PaloWriterTask

case class PaloLoadInfo(val jobId: Long, val label: String, val state: String, val errorMsg: String)

class PaloIllegalStateException(msg: String) extends Exception(msg)
