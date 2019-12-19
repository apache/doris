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

/**
 * read data from Doris BE to array.
 * @param partition Doris RDD partition
 * @param settings request configuration
 */
class ScalaValueReader(partition: PartitionDefinition, settings: Settings) {
  protected val logger = Logger.getLogger(classOf[ScalaValueReader])

  protected val client = new BackendClient(new Routing(partition.getBeAddress), settings)
  protected var offset = 0
  protected var eos: Boolean = false
  protected var rowBatch: RowBatch = _

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

    params.setBatch_size(batchSize)
    params.setQuery_timeout(queryDorisTimeout)
    params.setUser(settings.getProperty(DORIS_REQUEST_AUTH_USER, ""))
    params.setPasswd(settings.getProperty(DORIS_REQUEST_AUTH_PASSWORD, ""))

    logger.debug(s"Open scan params is, " +
        s"cluster: ${params.getCluster}, " +
        s"database: ${params.getDatabase}, " +
        s"table: ${params.getTable}, " +
        s"tabletId: ${params.getTablet_ids}, " +
        s"batch size: $batchSize, " +
        s"query timeout: $queryDorisTimeout, " +
        s"user: ${params.getUser}, " +
        s"query plan: ${params.opaqued_query_plan}")

    params
  }

  protected val openResult: TScanOpenResult = client.openScanner(openParams)
  protected val contextId: String = openResult.getContext_id
  protected val schema: Schema =
    SchemaUtils.convertToSchema(openResult.getSelected_columns)

  logger.debug(s"Open scan result is, contextId: $contextId, schema: $schema.")

  /**
   * read data and cached in rowBatch.
   * @return true if hax next value
   */
  def hasNext: Boolean = {
    if (!eos && (rowBatch == null || !rowBatch.hasNext)) {
      if (rowBatch != null) {
        offset += rowBatch.getReadRowCount
        rowBatch.close
      }
      val nextBatchParams = new TScanNextBatchParams
      nextBatchParams.setContext_id(contextId)
      nextBatchParams.setOffset(offset)
      val nextResult = client.getNext(nextBatchParams)
      eos = nextResult.isEos
      if (!eos) {
        rowBatch = new RowBatch(nextResult, schema)
      }
    }
    !eos
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
    client.closeScanner(closeParams)
  }
}
