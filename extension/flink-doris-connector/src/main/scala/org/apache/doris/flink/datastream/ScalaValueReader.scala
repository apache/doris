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

package org.apache.doris.flink.datastream

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.doris.flink.backend.BackendClient
import org.apache.doris.flink.cfg.ConfigurationOptions._
import org.apache.doris.flink.cfg.{DorisOptions, DorisReadOptions}
import org.apache.doris.flink.exception.ShouldNeverHappenException
import org.apache.doris.flink.rest.{PartitionDefinition, SchemaUtils}
import org.apache.doris.flink.rest.models.Schema
import org.apache.doris.flink.serialization.{Routing, RowBatch}
import org.apache.doris.flink.util.ErrorMessages
import org.apache.doris.flink.util.ErrorMessages._
import org.apache.doris.thrift.{TScanCloseParams, TScanNextBatchParams, TScanOpenParams, TScanOpenResult}
import org.apache.log4j.Logger

import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.control.Breaks

/**
 * read data from Doris BE to array.
 * @param partition Doris RDD partition
 * @param options request configuration
 */
class ScalaValueReader(partition: PartitionDefinition, options: DorisOptions, readOptions: DorisReadOptions) extends AutoCloseable {
  protected val logger = Logger.getLogger(classOf[ScalaValueReader])

  protected val client = new BackendClient(new Routing(partition.getBeAddress), readOptions)
  protected var offset = 0
  protected var eos: AtomicBoolean = new AtomicBoolean(false)
  protected var rowBatch: RowBatch = _
  // flag indicate if support deserialize Arrow to RowBatch asynchronously
  protected var deserializeArrowToRowBatchAsync: java.lang.Boolean = Try {
    if(readOptions.getDeserializeArrowAsync == null ) DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT else readOptions.getDeserializeArrowAsync
  } getOrElse {
    logger.warn(ErrorMessages.PARSE_BOOL_FAILED_MESSAGE, DORIS_DESERIALIZE_ARROW_ASYNC, readOptions.getDeserializeArrowAsync)
    DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT
  }

  protected var rowBatchBlockingQueue: BlockingQueue[RowBatch] = {
    val blockingQueueSize = Try {
      if(readOptions.getDeserializeQueueSize == null) DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT else readOptions.getDeserializeQueueSize
    } getOrElse {
      logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_DESERIALIZE_QUEUE_SIZE, readOptions.getDeserializeQueueSize)
      DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT
    }

    var queue: BlockingQueue[RowBatch] = null
    if (deserializeArrowToRowBatchAsync) {
      queue = new ArrayBlockingQueue(blockingQueueSize)
    }
    queue
  }

  private val openParams: TScanOpenParams = {
    val params = new TScanOpenParams
    params.cluster = DORIS_DEFAULT_CLUSTER
    params.database = partition.getDatabase
    params.table = partition.getTable

    params.tablet_ids = partition.getTabletIds.toList
    params.opaqued_query_plan = partition.getQueryPlan

    // max row number of one read batch
    val batchSize = Try {
      if(readOptions.getRequestBatchSize == null)  DORIS_BATCH_SIZE_DEFAULT else readOptions.getRequestBatchSize;
    } getOrElse {
        logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_BATCH_SIZE, readOptions.getRequestBatchSize)
        DORIS_BATCH_SIZE_DEFAULT
    }

    val queryDorisTimeout = Try {
      if(readOptions.getRequestQueryTimeoutS == null) DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT else readOptions.getRequestQueryTimeoutS
    } getOrElse {
      logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_REQUEST_QUERY_TIMEOUT_S, readOptions.getRequestQueryTimeoutS)
      DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT
    }

    val execMemLimit = Try {
      if(readOptions.getExecMemLimit == null) DORIS_EXEC_MEM_LIMIT_DEFAULT else readOptions.getExecMemLimit
    } getOrElse {
      logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_EXEC_MEM_LIMIT, readOptions.getExecMemLimit)
      DORIS_EXEC_MEM_LIMIT_DEFAULT
    }

    params.setBatchSize(batchSize)
    params.setQueryTimeout(queryDorisTimeout)
    params.setMemLimit(execMemLimit)
    params.setUser(options.getUsername)
    params.setPasswd(options.getPassword)

    logger.debug(s"Open scan params is, " +
        s"cluster: ${params.getCluster}, " +
        s"database: ${params.getDatabase}, " +
        s"table: ${params.getTable}, " +
        s"tabletId: ${params.getTabletIds}, " +
        s"batch size: $batchSize, " +
        s"query timeout: $queryDorisTimeout, " +
        s"execution memory limit: $execMemLimit, " +
        s"user: ${params.getUser}, " +
        s"query plan: ${params.getOpaquedQueryPlan}")

    params
  }

  protected val openResult: TScanOpenResult = client.openScanner(openParams)
  protected val contextId: String = openResult.getContextId
  protected val schema: Schema =
    SchemaUtils.convertToSchema(openResult.getSelectedColumns)

  protected val asyncThread: Thread = new Thread {
    override def run {
      val nextBatchParams = new TScanNextBatchParams
      nextBatchParams.setContextId(contextId)
      while (!eos.get) {
        nextBatchParams.setOffset(offset)
        val nextResult = client.getNext(nextBatchParams)
        eos.set(nextResult.isEos)
        if (!eos.get) {
          val rowBatch = new RowBatch(nextResult, schema).readArrow()
          offset += rowBatch.getReadRowCount
          rowBatch.close
          rowBatchBlockingQueue.put(rowBatch)
        }
      }
    }
  }

  protected val asyncThreadStarted: Boolean = {
    var started = false
    if (deserializeArrowToRowBatchAsync) {
      asyncThread.start
      started = true
    }
    started
  }

  logger.debug(s"Open scan result is, contextId: $contextId, schema: $schema.")

  /**
   * read data and cached in rowBatch.
   * @return true if hax next value
   */
  def hasNext: Boolean = {
    var hasNext = false
    if (deserializeArrowToRowBatchAsync && asyncThreadStarted) {
      // support deserialize Arrow to RowBatch asynchronously
      if (rowBatch == null || !rowBatch.hasNext) {
        val loop = new Breaks
        loop.breakable {
          while (!eos.get || !rowBatchBlockingQueue.isEmpty) {
            if (!rowBatchBlockingQueue.isEmpty) {
              rowBatch = rowBatchBlockingQueue.take
              hasNext = true
              loop.break
            } else {
              // wait for rowBatch put in queue or eos change
              Thread.sleep(5)
            }
          }
        }
      } else {
        hasNext = true
      }
    } else {
      // Arrow data was acquired synchronously during the iterative process
      if (!eos.get && (rowBatch == null || !rowBatch.hasNext)) {
        if (rowBatch != null) {
          offset += rowBatch.getReadRowCount
          rowBatch.close
        }
        val nextBatchParams = new TScanNextBatchParams
        nextBatchParams.setContextId(contextId)
        nextBatchParams.setOffset(offset)
        val nextResult = client.getNext(nextBatchParams)
        eos.set(nextResult.isEos)
        if (!eos.get) {
          rowBatch = new RowBatch(nextResult, schema).readArrow()
        }
      }
      hasNext = !eos.get
    }
    hasNext
  }

  /**
   * get next value.
   * @return next value
   */
  def next: java.util.List[_] = {
    if (!hasNext) {
      logger.error(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    rowBatch.next
  }

  def close(): Unit = {
    val closeParams = new TScanCloseParams
    closeParams.setContextId(contextId)
    client.closeScanner(closeParams)
  }

}
