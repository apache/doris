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

import java.util.Locale

/**
 * all of the configuration string use lower cases
 * contains two mode configuration: BULK_LOAD(Mini load) and LOAD

 * TODO: add the third mode: STREAMING_LOAD
 */
object PaloConfig {
  // common and necessary for palo
  val USERNAME: String = "username"
  val PASSWORD: String = "password"
  val DATABASE: String = "database"
  val TABLE: String = "table"
  val LEADERNODEURL: String = toLowerCase("leaderNodeUrl") // No effect, just a placeholder
  val COMPUTENODEURL: String = toLowerCase("computeNodeUrl")
  val MYSQLURL: String = toLowerCase("mysqlUrl")

  // common options for palo
  val CONNECTION_MAX_RETRIES: String = toLowerCase("connectionMaxRetries")
  val LOGIN_TIMEOUT_SEC: String = toLowerCase("loginTimeoutSec")
  val NETWORK_TIMEOUT_MS: String = toLowerCase("networkTimeoutMillis")
  val QUERY_MAX_RETRIES: String = toLowerCase("queryMaxRetries")
  val QUERY_TIMEOUT_SEC: String = toLowerCase("queryTimeoutSec")
  val DEFAULT_RETRY_INTERVAL_MS: String = toLowerCase("defaultRetryIntervalMs")
  val QUERY_RETRY_INTERVAL_MS: String = toLowerCase("queryRetryIntervalMs")
  val DELETE_FILE: String = toLowerCase("deleteFile")
  val LOADWAY: String = toLowerCase("loadWay")

  // options for LOAD properties
  val PALO_TIME_OUT: String = "timeout"
  val MAX_FILTER_RATIO: String = "max_filter_ratio"
  val LOAD_DELETE_FLAG: String = "load_delete_flag"
  val IS_NEGATIVE: String = "is_negative"

  // specific for "LOAD" way
  val PALO_DATA_DIR: String = "palo.data.dir"
  val LOADCMD: String = "loadcmd"
  val BROKER_NAME: String = toLowerCase("brokerName")

  // specific for "BULK_LOAD(Mini load)" way
  val HTTP_MAX_RETRIES: String = toLowerCase("httpMaxRetries")
  val HTTP_CONNECT_TIMEOUT_MS: String = toLowerCase("httpConnectTimeoutMs")
  val HTTP_READ_TIMEOUT_MS: String = toLowerCase("httpReadTimeoutMs")
  val HTTP_PORT: String = toLowerCase("httpPort")
  val BULK_LOAD_READ_BUFFERSIZE: String = toLowerCase("bulkLoadReadBufferSize")
  val SEPARATOR: String = "separator"

  private def toLowerCase(name: String): String = {
    name.toLowerCase(Locale.ROOT)
  }
}
