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
import org.apache.spark.sql.{AnalysisException, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
 * The provider class for the [[PaloSink]].
 * Check the necessary parameters as soon as the job start on driver.
 * If the parameters miss, will throw IllegalArgumentException.
 */
private[palo] class PaloSinkProvider extends StreamSinkProvider
    with DataSourceRegister with Logging {

  override def shortName(): String = "palo"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    checkValidity(parameters, outputMode)
    new PaloSink(sqlContext, parameters)
  }

  /**
   * check necessary parameters and output mode(only support APPEND and UPDATE model)
   *
   * @param parameters  user specified parameters
   * @param outputMode  user specified mode, only support APPEND/UPDATE mode
   */
  def checkValidity(parameters: Map[String, String], outputMode: OutputMode) {
    check(PaloConfig.COMPUTENODEURL, parameters)
    check(PaloConfig.MYSQLURL, parameters)
    check(PaloConfig.USERNAME, parameters)
    check(PaloConfig.PASSWORD, parameters)
    check(PaloConfig.DATABASE, parameters)
    check(PaloConfig.TABLE, parameters)

    if(outputMode == OutputMode.Complete) {
      throw new IllegalArgumentException("outputMode must be Append/Update for palo sink")
    }
  }

  private def check(key: String, parameters: Map[String, String]) {
    if (!parameters.contains(key) || parameters(key).isEmpty ) {
      throw new IllegalArgumentException(s"$key must be specified!")
    }
  }
}
