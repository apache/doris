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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.HoodieBaseRelation.{BaseFileReader, projectReader}
import org.apache.hudi.MergeOnReadSnapshotRelation.isProjectionCompatible
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.{DataSourceReadOptions, HoodieMergeOnReadFileSplit, HoodieTableSchema, HoodieTableState, LogFileIterator, RecordMergingFileIterator}
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow

import java.io.Closeable
import java.util.function.Predicate

/**
 * Class holding base-file readers for 3 different use-cases:
 *
 * <ol>
 * <li>Full-schema reader: is used when whole row has to be read to perform merging correctly.
 * This could occur, when no optimizations could be applied and we have to fallback to read the whole row from
 * the base file and the corresponding delta-log file to merge them correctly</li>
 *
 * <li>Required-schema reader: is used when it's fine to only read row's projected columns.
 * This could occur, when row could be merged with corresponding delta-log record while leveraging only
 * projected columns</li>
 *
 * <li>Required-schema reader (skip-merging): is used when when no merging will be performed (skip-merged).
 * This could occur, when file-group has no delta-log files</li>
 * </ol>
 */
private[hudi] case class HoodieMergeOnReadBaseFileReaders(fullSchemaReader: BaseFileReader,
                                                          requiredSchemaReader: BaseFileReader,
                                                          requiredSchemaReaderSkipMerging: BaseFileReader)

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
 * Base file as well as all of the Delta Log files simply returning concatenation of these streams, while not
 * performing any combination/merging of the records w/ the same primary keys (ie producing duplicates potentially)
 */
private class SkipMergeIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileReader: BaseFileReader,
                                dataSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                tableState: HoodieTableState,
                                config: Configuration)
  extends LogFileIterator(split, dataSchema, requiredSchema, tableState, config) {

  private val requiredSchemaProjection = generateUnsafeProjection(baseFileReader.schema, structTypeSchema)

  private val baseFileIterator = baseFileReader(split.dataFile.get)

  override def doHasNext: Boolean = {
    if (baseFileIterator.hasNext) {
      // No merge is required, simply load current row and project into required schema
      nextRecord = requiredSchemaProjection(baseFileIterator.next())
      true
    } else {
      super[LogFileIterator].doHasNext
    }
  }
}

/**
 * Reference to Apache Hudi
 * see <a href="https://github.com/apache/hudi/blob/release-0.13.0/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieMergeOnReadRDD.scala">HoodieMergeOnReadRDD</a>
 */
class HoodieMORRecordIterator(config: Configuration,
                              fileReaders: HoodieMergeOnReadBaseFileReaders,
                              tableSchema: HoodieTableSchema,
                              requiredSchema: HoodieTableSchema,
                              tableState: HoodieTableState,
                              mergeType: String,
                              fileSplit: HoodieMergeOnReadFileSplit,
                              includeStartTime: Boolean = false,
                              startTimestamp: String = null,
                              endTimestamp: String = null) extends Iterator[InternalRow] with Closeable {
  protected val maxCompactionMemoryInBytes: Long = config.getLongBytes(
    "hoodie.compaction.memory", 512 * 1024 * 1024)

  protected val recordIterator: Iterator[InternalRow] = {
    val iter = fileSplit match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty =>
        val projectedReader = projectReader(fileReaders.requiredSchemaReaderSkipMerging, requiredSchema.structTypeSchema)
        projectedReader(dataFileOnlySplit.dataFile.get)

      case logFileOnlySplit if logFileOnlySplit.dataFile.isEmpty =>
        new LogFileIterator(logFileOnlySplit, tableSchema, requiredSchema, tableState, config)

      case split => mergeType match {
        case DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL =>
          val reader = fileReaders.requiredSchemaReaderSkipMerging
          new SkipMergeIterator(split, reader, tableSchema, requiredSchema, tableState, config)

        case DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL =>
          val reader = pickBaseFileReader()
          new RecordMergingFileIterator(split, reader, tableSchema, requiredSchema, tableState, config)

        case _ => throw new UnsupportedOperationException(s"Not supported merge type ($mergeType)")
      }
    }

    val commitTimeMetadataFieldIdx = requiredSchema.structTypeSchema.fieldNames.indexOf(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
    val needsFiltering = commitTimeMetadataFieldIdx >= 0 && !StringUtils.isNullOrEmpty(startTimestamp) && !StringUtils.isNullOrEmpty(endTimestamp)
    if (needsFiltering) {
      val filterT: Predicate[InternalRow] = getCommitTimeFilter(includeStartTime, commitTimeMetadataFieldIdx)
      iter.filter(filterT.test)
    }
    else {
      iter
    }
  }

  private def getCommitTimeFilter(includeStartTime: Boolean, commitTimeMetadataFieldIdx: Int): Predicate[InternalRow] = {
    if (includeStartTime) {
      new Predicate[InternalRow] {
        override def test(row: InternalRow): Boolean = {
          val commitTime = row.getString(commitTimeMetadataFieldIdx)
          commitTime >= startTimestamp && commitTime <= endTimestamp
        }
      }
    } else {
      new Predicate[InternalRow] {
        override def test(row: InternalRow): Boolean = {
          val commitTime = row.getString(commitTimeMetadataFieldIdx)
          commitTime > startTimestamp && commitTime <= endTimestamp
        }
      }
    }
  }

  private def pickBaseFileReader(): BaseFileReader = {
    // NOTE: This is an optimization making sure that even for MOR tables we fetch absolute minimum
    //       of the stored data possible, while still properly executing corresponding relation's semantic
    //       and meet the query's requirements.
    //
    //       Here we assume that iff queried table does use one of the standard (and whitelisted)
    //       Record Payload classes then we can avoid reading and parsing the records w/ _full_ schema,
    //       and instead only rely on projected one, nevertheless being able to perform merging correctly
    if (isProjectionCompatible(tableState)) {
      fileReaders.requiredSchemaReader
    } else {
      fileReaders.fullSchemaReader
    }
  }

  override def hasNext: Boolean = {
    recordIterator.hasNext
  }

  override def next(): InternalRow = {
    recordIterator.next()
  }

  override def close(): Unit = {
    recordIterator match {
      case closeable: Closeable =>
        closeable.close()
      case _ =>
    }
  }
}
