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

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, SQLTimeoutException, Timestamp}
import java.util.concurrent.{Executors, ExecutorService, TimeUnit}

import org.apache.spark.internal.Logging

/**
 * use to access Palo by MYSQL protocol
 *
 * TODO: make the PaloClient can be shared by tasks on the same Executor
 */
private[palo] class PaloClient(parameters: Map[String, String]) extends Logging {
  private[palo] val connectionMaxRetries =
    parameters.getOrElse(PaloConfig.CONNECTION_MAX_RETRIES, "2").toInt
  private[palo] val loginTimeoutSec =
    parameters.getOrElse(PaloConfig.LOGIN_TIMEOUT_SEC, "30").toInt
  private[palo] val networkTimeoutMillis =
    parameters.getOrElse(PaloConfig.NETWORK_TIMEOUT_MS, "300000").toInt // 5 min
  private[palo] val queryTimeoutSec =
    parameters.getOrElse(PaloConfig.QUERY_TIMEOUT_SEC, "30").toInt

  private[palo] val driverName = "com.mysql.jdbc.Driver"
  private[palo] val prefix = "jdbc:mysql://"
  private[palo] val suffix = "?useUnicode=true&characterEncoding=UTF8"
  private[palo] val username: String = parameters(PaloConfig.USERNAME)
  private[palo] val password: String = parameters(PaloConfig.PASSWORD)
  private[palo] val database: String = parameters(PaloConfig.DATABASE)
  private[palo] val table: String = parameters(PaloConfig.TABLE)
  private[palo] val url = s"${prefix}${parameters(PaloConfig.MYSQLURL)}/${database}${suffix}"
  private[palo] var executor: ExecutorService = Executors.newFixedThreadPool(1)
  private[palo] var connection: Connection = null

  private[palo] def initConnection(): Connection = {
    assert(connectionMaxRetries >= 0, "connectionMaxRetries can not be negative!")
    logDebug(s"connect with ${url}")

    var connection: Connection = null
    var count = 0
    while (count <= connectionMaxRetries) {
      try {
        // scalastyle:off
        Class.forName(driverName)
        // scalastyle:on
        DriverManager.setLoginTimeout(loginTimeoutSec * (count + 1))
        connection = DriverManager.getConnection(url,
            username, password)

        // setNetworkTimeout is necessary and should be given
        // a high enough value so it is never triggered before any normal timeout,
        // such as transaction timeouts(https://docs.oracle.com/javase/8/docs/api/)
        connection.setNetworkTimeout(executor, networkTimeoutMillis)
        count = connectionMaxRetries + 1
      } catch {
        case e: SQLTimeoutException =>
          connection = null
          count += 1
          if (count <= connectionMaxRetries) {
            logWarning(s"initConnection fail for ${e.getMessage}, retry time: $count")
          }

        case e: SQLException =>
          connection = null
          count = connectionMaxRetries + 1
          logError(s"initConnection fail for ${e.getMessage}")
          e.printStackTrace()
      }
    }
    assert(connection != null, "connection must not be null!")
    logDebug(s"init Connection successfully, loginTimeout:${DriverManager.getLoginTimeout()} " +
      s"seconds, networkTimeout ${connection.getNetworkTimeout()} milliseconds")
    connection
  }

  def start() {
    connection = initConnection
  }

  def getPreparedStatement(sql: String): PreparedStatement = {
    var preparedStatement: PreparedStatement = null
    try {
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.setQueryTimeout(queryTimeoutSec)
    } catch {
      case e: SQLException => logError(s"getPreparedStatement fail for ${e.getMessage}")
    }
    assert(preparedStatement != null, "preparedStatement must not be null!")
    logDebug(s"queryTimeout:${preparedStatement.getQueryTimeout}")

    preparedStatement
  }

  def close() {
    try {
      // Shutdown EexecutorService before close connection.
      // If not, because 'setNetworkTimeout' is a asynchronous process, connection.close
      // may run before 'setNetworkTimeout' process completed, which may
      // clause NullPointerException or java.net.SocketException.
      closeExecutor

      if (connection != null)  {
        connection.close()
        connection = null
      }
      logInfo("close connection to Palo successfully")
    } catch {
      case e: SQLException => logError(s"close connection faild: ${e.getMessage}")
    }
  }

  // close executor
  def closeExecutor() {
    executor.shutdown()
    try {
      while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        logWarning("executor.awaitTermiantion timeout, try again!");
        executor.shutdownNow();
      }
    } catch {
      case e: InterruptedException =>
        e.printStackTrace
    }
  }
} // end class of PaloClient
