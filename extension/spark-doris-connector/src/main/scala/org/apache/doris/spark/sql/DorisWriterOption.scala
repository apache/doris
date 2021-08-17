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

import org.apache.doris.spark.exception.DorisException

class DorisWriterOption(val feHostPort: String ,val dbName: String,val tbName: String,
                        val  user: String ,val password: String,
                        val maxRowCount: Long,val maxRetryTimes:Int)

object DorisWriterOption{
 def apply(parameters: Map[String, String]): DorisWriterOption={
  val feHostPort: String = parameters.getOrElse(DorisWriterOptionKeys.feHostPort, throw new DorisException("feHostPort is empty"))

  val dbName: String = parameters.getOrElse(DorisWriterOptionKeys.dbName, throw new DorisException("dbName is empty"))

  val tbName: String = parameters.getOrElse(DorisWriterOptionKeys.tbName, throw new DorisException("tbName is empty"))

  val user: String = parameters.getOrElse(DorisWriterOptionKeys.user, throw new DorisException("user is empty"))

  val password: String = parameters.getOrElse(DorisWriterOptionKeys.password, throw new DorisException("password is empty"))

  val maxRowCount: Long = parameters.getOrElse(DorisWriterOptionKeys.maxRowCount, "1024").toLong
  val maxRetryTimes: Int = parameters.getOrElse(DorisWriterOptionKeys.maxRetryTimes, "3").toInt
  new DorisWriterOption(feHostPort, dbName, tbName, user, password, maxRowCount, maxRetryTimes)
 }
}