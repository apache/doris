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

package org.apache.doris.spark.rdd

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent._
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}

import scala.collection.JavaConversions._
import scala.util.Try
import org.apache.doris.spark.backend.BackendClient
import org.apache.doris.spark.cfg.ConfigurationOptions._
import org.apache.doris.spark.cfg.Settings
import org.apache.doris.spark.exception.ShouldNeverHappenException
import org.apache.doris.spark.rest.PartitionDefinition
import org.apache.doris.spark.rest.models.Schema
import org.apache.doris.spark.serialization.{Routing, RowBatch}
import org.apache.doris.spark.sql.SchemaUtils
import org.apache.doris.spark.util.ErrorMessages
import org.apache.doris.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE
import org.apache.doris.thrift.{TScanCloseParams, TScanNextBatchParams, TScanOpenParams, TScanOpenResult}
import org.apache.log4j.Logger

import scala.util.control.Breaks

/**
 * read data from Doris BE to array.
 * @param partition Doris RDD partition
 * @param settings request configuration
 */
class ScalaValueReader(partition: PartitionDefinition, settings: Settings) {
  protected val logger = Logger.getLogger(classOf[ScalaValueReader])

  protected val client = new BackendClient(new Routing(partition.getBeAddress), settings)
  protected val clientLock =
    if (deserializeArrowToRowBatchAsync) new ReentrantLock()
    else new NoOpLock
  protected var offset = 0
  protected var eos: AtomicBoolean = new AtomicBoolean(false)
  protected var rowBatch: RowBatch = _
  // flag indicate if support deserialize Arrow to RowBatch asynchronously
  protected lazy val deserializeArrowToRowBatchAsync: Boolean = Try {
    settings.getProperty(DORIS_DESERIALIZE_ARROW_ASYNC, DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT.toString).toBoolean
  } getOrElse {
    logger.warn(ErrorMessages.PARSE_BOOL_FAILED_MESSAGE, DORIS_DESERIALIZE_ARROW_ASYNC, settings.getProperty(DORIS_DESERIALIZE_ARROW_ASYNC))
    DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT
  }

  protected var rowBatchBlockingQueue: BlockingQueue[RowBatch] = {
    val blockingQueueSize = Try {
      settings.getProperty(DORIS_DESERIALIZE_QUEUE_SIZE, DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT.toString).toInt
    } getOrElse {
      logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_DESERIALIZE_QUEUE_SIZE, settings.getProperty(DORIS_DESERIALIZE_QUEUE_SIZE))
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
      settings.getProperty(DORIS_BATCH_SIZE, DORIS_BATCH_SIZE_DEFAULT.toString).toInt
    } getOrElse {
        logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_BATCH_SIZE, settings.getProperty(DORIS_BATCH_SIZE))
        DORIS_BATCH_SIZE_DEFAULT
    }

    val queryDorisTimeout = Try {
      settings.getProperty(DORIS_REQUEST_QUERY_TIMEOUT_S, DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT.toString).toInt
    } getOrElse {
      logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_REQUEST_QUERY_TIMEOUT_S, settings.getProperty(DORIS_REQUEST_QUERY_TIMEOUT_S))
      DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT
    }

    val execMemLimit = Try {
      settings.getProperty(DORIS_EXEC_MEM_LIMIT, DORIS_EXEC_MEM_LIMIT_DEFAULT.toString).toLong
    } getOrElse {
      logger.warn(ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE, DORIS_EXEC_MEM_LIMIT, settings.getProperty(DORIS_EXEC_MEM_LIMIT))
      DORIS_EXEC_MEM_LIMIT_DEFAULT
    }

    params.setBatch_size(batchSize)
    params.setQuery_timeout(queryDorisTimeout)
    params.setMem_limit(execMemLimit)
    params.setUser(settings.getProperty(DORIS_REQUEST_AUTH_USER, ""))
    params.setPasswd(settings.getProperty(DORIS_REQUEST_AUTH_PASSWORD, ""))

    logger.debug(s"Open scan params is, " +
        s"cluster: ${params.getCluster}, " +
        s"database: ${params.getDatabase}, " +
        s"table: ${params.getTable}, " +
        s"tabletId: ${params.getTablet_ids}, " +
        s"batch size: $batchSize, " +
        s"query timeout: $queryDorisTimeout, " +
        s"execution memory limit: $execMemLimit, " +
        s"user: ${params.getUser}, " +
        s"query plan: ${params.opaqued_query_plan}")

    params
  }

  protected val openResult: TScanOpenResult = lockClient(_.openScanner(openParams))
  protected val contextId: String = openResult.getContext_id
  protected val schema: Schema =
    SchemaUtils.convertToSchema(openResult.getSelected_columns)

  protected val asyncThread: Thread = new Thread {
    override def run {
      val nextBatchParams = new TScanNextBatchParams
      nextBatchParams.setContext_id(contextId)
      while (!eos.get) {
        nextBatchParams.setOffset(offset)
        val nextResult = lockClient(_.getNext(nextBatchParams))
        eos.set(nextResult.isEos)
        if (!eos.get) {
          val rowBatch = new RowBatch(nextResult, schema)
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
        nextBatchParams.setContext_id(contextId)
        nextBatchParams.setOffset(offset)
        val nextResult = lockClient(_.getNext(nextBatchParams))
        eos.set(nextResult.isEos)
        if (!eos.get) {
          rowBatch = new RowBatch(nextResult, schema)
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
  def next: AnyRef = {
    if (!hasNext) {
      logger.error(SHOULD_NOT_HAPPEN_MESSAGE)
      throw new ShouldNeverHappenException
    }
    rowBatch.next
  }

  def close(): Unit = {
    val closeParams = new TScanCloseParams
    closeParams.context_id = contextId
    lockClient(_.closeScanner(closeParams))
  }

  private def lockClient[T](action: BackendClient => T): T = {
    clientLock.lock()
    try {
      action(client)
    } finally {
      clientLock.unlock()
    }
  }

  private class NoOpLock extends Lock {
    override def lock(): Unit = {}

    override def lockInterruptibly(): Unit = {}

    override def tryLock(): Boolean = true

    override def tryLock(time: Long, unit: TimeUnit): Boolean = true

    override def unlock(): Unit = {}

    override def newCondition(): Condition = {
      throw new UnsupportedOperationException("NoOpLock can't provide a condition")
    }
  }
}
