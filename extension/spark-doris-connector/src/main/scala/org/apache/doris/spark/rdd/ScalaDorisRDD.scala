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

import scala.reflect.ClassTag

import org.apache.doris.spark.cfg.ConfigurationOptions.DORIS_VALUE_READER_CLASS
import org.apache.doris.spark.cfg.Settings
import org.apache.doris.spark.rest.PartitionDefinition

import org.apache.spark.{Partition, SparkContext, TaskContext}

private[spark] class ScalaDorisRDD[T: ClassTag](
    sc: SparkContext,
    params: Map[String, String] = Map.empty)
    extends AbstractDorisRDD[T](sc, params) {
  override def compute(split: Partition, context: TaskContext): ScalaDorisRDDIterator[T] = {
    new ScalaDorisRDDIterator(context, split.asInstanceOf[DorisPartition].dorisPartition)
  }
}

private[spark] class ScalaDorisRDDIterator[T](
    context: TaskContext,
    partition: PartitionDefinition)
    extends AbstractDorisRDDIterator[T](context, partition) {

  override def initReader(settings: Settings) = {
    settings.setProperty(DORIS_VALUE_READER_CLASS, classOf[ScalaValueReader].getName)
  }

  override def createValue(value: Object): T = {
    value.asInstanceOf[T]
  }
}
