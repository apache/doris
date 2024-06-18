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

package org.apache.doris.hudi

import org.apache.hudi.HoodieTableSchema
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._

/**
 * Reference to Apache Hudi
 * see <a href="https://github.com/apache/hudi/blob/release-0.14.1/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/MergeOnReadIncrementalRelation.scala">MergeOnReadIncrementalRelation</a>
 */
class MORIncrementalSplitReader(override val split: HoodieSplit) extends MORSnapshotSplitReader(split) with IncrementalSplitReaderTrait {

  override protected def composeIterator(tableSchema: HoodieTableSchema,
                                         requiredSchema: HoodieTableSchema,
                                         requestedColumns: Array[String],
                                         filters: Array[Filter]): Iterator[InternalRow] = {
    // The only required filters are ones that make sure we're only fetching records that
    // fall into incremental span of the timeline being queried
    val requiredFilters = incrementalSpanRecordFilters
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)

    new HoodieMORRecordIterator(split.hadoopConf,
      readers,
      tableSchema,
      requiredSchema,
      tableState,
      mergeType,
      getFileSplit(),
      includeStartTime = includeStartTime,
      startTimestamp = startTs,
      endTimestamp = endTs)
  }

}

/**
 * Reference to Apache Hudi
 * see <a href="https://github.com/apache/hudi/blob/release-0.14.1/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/MergeOnReadIncrementalRelation.scala">HoodieIncrementalRelationTrait</a>
 */
trait IncrementalSplitReaderTrait extends BaseSplitReader {
  protected val includeStartTime: Boolean = "true".equalsIgnoreCase(optParams("hoodie.datasource.read.incr.includeStartTime"))
  protected val startTs: String = optParams("hoodie.datasource.read.begin.instanttime")
  protected val endTs: String = optParams("hoodie.datasource.read.end.instanttime")

  // Record filters making sure that only records w/in the requested bounds are being fetched as part of the
  // scan collected by this relation
  protected lazy val incrementalSpanRecordFilters: Seq[Filter] = {
    val isNotNullFilter = IsNotNull(HoodieRecord.COMMIT_TIME_METADATA_FIELD)

    val largerThanFilter = if (includeStartTime) {
      GreaterThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, startTs)
    } else {
      GreaterThan(HoodieRecord.COMMIT_TIME_METADATA_FIELD, startTs)
    }

    val lessThanFilter = LessThanOrEqual(HoodieRecord.COMMIT_TIME_METADATA_FIELD, endTs)

    Seq(isNotNullFilter, largerThanFilter, lessThanFilter)
  }

  override lazy val mandatoryFields: Seq[String] = {
    // NOTE: This columns are required for Incremental flow to be able to handle the rows properly, even in
    //       cases when no columns are requested to be fetched (for ex, when using {@code count()} API)
    Seq(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.COMMIT_TIME_METADATA_FIELD) ++
      preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())
  }
}
