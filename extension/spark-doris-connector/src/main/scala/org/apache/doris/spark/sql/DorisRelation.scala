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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.math.min

import org.apache.doris.spark.cfg.ConfigurationOptions._
import org.apache.doris.spark.cfg.{ConfigurationOptions, SparkSettings}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}


private[sql] class DorisRelation(
    val sqlContext: SQLContext, parameters: Map[String, String])
    extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan {

  private lazy val cfg = {
    val conf = new SparkSettings(sqlContext.sparkContext.getConf)
    conf.merge(parameters.asJava)
    conf
  }

  private lazy val inValueLengthLimit =
    min(cfg.getProperty(DORIS_FILTER_QUERY_IN_MAX_COUNT, "100").toInt,
      DORIS_FILTER_QUERY_IN_VALUE_UPPER_LIMIT)

  private lazy val lazySchema = SchemaUtils.discoverSchema(cfg)

  private lazy val dialect = JdbcDialects.get("")

  override def schema: StructType = lazySchema

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(Utils.compileFilter(_, dialect, inValueLengthLimit).isEmpty)
  }

  // TableScan
  override def buildScan(): RDD[Row] = buildScan(Array.empty)

  // PrunedScan
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan(requiredColumns, Array.empty)

  // PrunedFilteredScan
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val paramWithScan = mutable.LinkedHashMap[String, String]() ++ parameters

    // filter where clause can be handled by Doris BE
    val filterWhereClause: String = {
      filters.flatMap(Utils.compileFilter(_, dialect, inValueLengthLimit))
          .map(filter => s"($filter)").mkString(" and ")
    }

    // required columns for column pruner
    if (requiredColumns != null && requiredColumns.length > 0) {
      paramWithScan += (ConfigurationOptions.DORIS_READ_FIELD ->
          requiredColumns.map(Utils.quote).mkString(","))
    } else {
      paramWithScan += (ConfigurationOptions.DORIS_READ_FIELD ->
          lazySchema.fields.map(f => f.name).mkString(","))
    }

    if (filters != null && filters.length > 0) {
      paramWithScan += (ConfigurationOptions.DORIS_FILTER_QUERY -> filterWhereClause)
    }

    new ScalaDorisRowRDD(sqlContext.sparkContext, paramWithScan.toMap, lazySchema)
  }
}
