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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row

private[spark] class ScalaDorisRow(rowOrder: Seq[String]) extends Row {
   lazy val values: ArrayBuffer[Any] = ArrayBuffer.fill(rowOrder.size)(null)

  /** No-arg constructor for Kryo serialization. */
  def this() = this(null)

  def iterator = values.iterator

  override def length: Int = values.length

  override def apply(i: Int): Any = values(i)

  override def get(i: Int): Any = values(i)

  override def isNullAt(i: Int): Boolean = values(i) == null

  override def getInt(i: Int): Int = getAs[Int](i)

  override def getLong(i: Int): Long = getAs[Long](i)

  override def getDouble(i: Int): Double = getAs[Double](i)

  override def getFloat(i: Int): Float = getAs[Float](i)

  override def getBoolean(i: Int): Boolean = getAs[Boolean](i)

  override def getShort(i: Int): Short = getAs[Short](i)

  override def getByte(i: Int): Byte = getAs[Byte](i)

  override def getString(i: Int): String = get(i).toString()

  override def copy(): Row = this

  override def toSeq = values.toSeq
}
