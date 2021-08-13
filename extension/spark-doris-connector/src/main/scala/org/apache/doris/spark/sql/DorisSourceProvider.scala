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

import java.util.StringJoiner

import org.apache.commons.collections.CollectionUtils
import org.apache.doris.spark.DorisStreamLoad
import org.apache.doris.spark.cfg.SparkSettings
import org.apache.doris.spark.exception.DorisException
import org.apache.doris.spark.rest.RestService
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, Filter, RelationProvider}
import org.apache.spark.sql.types.StructType
import org.json4s.jackson.Json

import scala.collection.mutable.ListBuffer
import scala.util.Random

private[sql] class DorisSourceProvider extends DataSourceRegister with RelationProvider with CreatableRelationProvider with Logging {
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
    val dorisWriterOption = getDorisWriterOption(parameters)
    val sparkSettings = new SparkSettings(sqlContext.sparkContext.getConf)
    // choose available be node
    val choosedBeHost = RestService.randomBackend(sparkSettings,dorisWriterOption,log)
    // init stream loader
    val dorisStreamLoader = new DorisStreamLoad(choosedBeHost, dorisWriterOption.dbName, dorisWriterOption.tbName, dorisWriterOption.user, dorisWriterOption.password)
    val fieldDelimiter: String = "\t"
    val lineDelimiter: String = "\n"
    val NULL_VALUE: String = "\\N"
    data.foreachPartition(partition => {

      val buffer = ListBuffer[String]()
      partition.foreach(row => {
        val value = new StringJoiner(fieldDelimiter)
        // create one row string
        for (i <- 0 until row.size) {
          val field = row.get(i)
          if (field == null) {
            value.add(NULL_VALUE)
          } else {
            value.add(field.toString)
          }
        }
        // add one row string to buffer
        buffer += value.toString
        if (buffer.size > dorisWriterOption.maxRowCount) {
          dorisStreamLoader.load(buffer.mkString(lineDelimiter))
          buffer.clear()
        }
      })
      // flush buffer
      if (buffer.nonEmpty) {
        dorisStreamLoader.load(buffer.mkString(lineDelimiter))
        buffer.clear()
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


  def getDorisWriterOption(parameters: Map[String, String])={
    val feHostPort: String = parameters.getOrElse(DorisWriterOptionKeys.feHostPort, throw new DorisException("feHostPort is empty"))

    val dbName: String = parameters.getOrElse(DorisWriterOptionKeys.dbName, throw new DorisException("dbName is empty"))

    val tbName: String = parameters.getOrElse(DorisWriterOptionKeys.tbName, throw new DorisException("tbName is empty"))

    val user: String = parameters.getOrElse(DorisWriterOptionKeys.user, throw new DorisException("user is empty"))

    val password: String = parameters.getOrElse(DorisWriterOptionKeys.password, throw new DorisException("password is empty"))

    val maxRowCount: Long = parameters.getOrElse(DorisWriterOptionKeys.maxRowCount, "1024").toLong

    DorisWriterOption(feHostPort,dbName,tbName,user,password,maxRowCount)
  }


}
