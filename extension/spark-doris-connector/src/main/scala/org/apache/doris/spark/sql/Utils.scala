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

import org.apache.commons.lang3.StringUtils
import org.apache.doris.spark.cfg.ConfigurationOptions
import org.apache.doris.spark.exception.DorisException
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._
import org.slf4j.Logger

import java.sql.{Date, Timestamp}

private[sql] object Utils {
  /**
   * quote column name
   * @param colName column name
   * @return quoted column name
   */
  def quote(colName: String): String = s"`$colName`"

  /**
   * compile a filter to Doris FE filter format.
   * @param filter filter to be compile
   * @param dialect jdbc dialect to translate value to sql format
   * @param inValueLengthLimit max length of in value array
   * @return if Doris FE can handle this filter, return None if Doris FE can not handled it.
   */
  def compileFilter(filter: Filter, dialect: JdbcDialect, inValueLengthLimit: Int): Option[String] = {
    Option(filter match {
      case EqualTo(attribute, value) => s"${quote(attribute)} = ${compileValue(value)}"
      case GreaterThan(attribute, value) => s"${quote(attribute)} > ${compileValue(value)}"
      case GreaterThanOrEqual(attribute, value) => s"${quote(attribute)} >= ${compileValue(value)}"
      case LessThan(attribute, value) => s"${quote(attribute)} < ${compileValue(value)}"
      case LessThanOrEqual(attribute, value) => s"${quote(attribute)} <= ${compileValue(value)}"
      case In(attribute, values) =>
        if (values.isEmpty || values.length >= inValueLengthLimit) {
          null
        } else {
          s"${quote(attribute)} in (${compileValue(values)})"
        }
      case IsNull(attribute) => s"${quote(attribute)} is null"
      case IsNotNull(attribute) => s"${quote(attribute)} is not null"
      case And(left, right) =>
        val and = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" and ")
        } else {
          null
        }
      case Or(left, right) =>
        val or = Seq(left, right).flatMap(compileFilter(_, dialect, inValueLengthLimit))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" or ")
        } else {
          null
        }
      case _ => null
    })
  }

  /**
   * Escape special characters in SQL string literals.
   * @param value The string to be escaped.
   * @return Escaped string.
   */
  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

  /**
   * Converts value to SQL expression.
   * @param value The value to be converted.
   * @return Converted value.
   */
  private def compileValue(value: Any): Any = value match {
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "'" + timestampValue + "'"
    case dateValue: Date => "'" + dateValue + "'"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }

  /**
   * check parameters validation and process it.
   * @param parameters parameters from rdd and spark conf
   * @param logger slf4j logger
   * @return processed parameters
   */
  def params(parameters: Map[String, String], logger: Logger) = {
    // '.' seems to be problematic when specifying the options
    val dottedParams = parameters.map { case (k, v) => (k.replace('_', '.'), v)}

    val preferredTableIdentifier = dottedParams.get(ConfigurationOptions.DORIS_TABLE_IDENTIFIER)
      .orElse(dottedParams.get(ConfigurationOptions.TABLE_IDENTIFIER))
    logger.debug(s"preferred Table Identifier is '$preferredTableIdentifier'.")

    // Convert simple parameters into internal properties, and prefix other parameters
    // Convert password parameters from "password" into internal password properties
    // reuse credentials mask method in spark ExternalCatalogUtils￿#maskCredentials
    val processedParams = dottedParams.map {
      case (ConfigurationOptions.DORIS_PASSWORD, _) =>
        logger.error(s"${ConfigurationOptions.DORIS_PASSWORD} cannot use in Doris Datasource.")
        throw new DorisException(s"${ConfigurationOptions.DORIS_PASSWORD} cannot use in Doris Datasource," +
          s" use 'password' option to set password.")
      case (ConfigurationOptions.DORIS_USER, _) =>
        logger.error(s"${ConfigurationOptions.DORIS_USER} cannot use in Doris Datasource.")
        throw new DorisException(s"${ConfigurationOptions.DORIS_USER} cannot use in Doris Datasource," +
          s" use 'user' option to set user.")
      case (k, v) =>
        if (k.startsWith("doris.")) (k, v)
        else ("doris." + k, v)
    }.map{
      case (ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD, _) =>
        logger.error(s"${ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD} cannot use in Doris Datasource.")
        throw new DorisException(s"${ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD} cannot use in" +
          s" Doris Datasource, use 'password' option to set password.")
      case (ConfigurationOptions.DORIS_REQUEST_AUTH_USER, _) =>
        logger.error(s"${ConfigurationOptions.DORIS_REQUEST_AUTH_USER} cannot use in Doris Datasource.")
        throw new DorisException(s"${ConfigurationOptions.DORIS_REQUEST_AUTH_USER} cannot use in" +
          s" Doris Datasource, use 'user' option to set user.")
      case (ConfigurationOptions.DORIS_PASSWORD, v) =>
        (ConfigurationOptions.DORIS_REQUEST_AUTH_PASSWORD, v)
      case (ConfigurationOptions.DORIS_USER, v) =>
        (ConfigurationOptions.DORIS_REQUEST_AUTH_USER, v)
      case (k, v) => (k, v)
    }

    // Set the preferred resource if it was specified originally
    val finalParams = preferredTableIdentifier match {
      case Some(tableIdentifier) => processedParams + (ConfigurationOptions.DORIS_TABLE_IDENTIFIER -> tableIdentifier)
      case None => processedParams
    }

    // validate path is available
    finalParams.getOrElse(ConfigurationOptions.DORIS_TABLE_IDENTIFIER,
        throw new DorisException("table identifier must be specified for doris table identifier."))

    finalParams
  }
}
