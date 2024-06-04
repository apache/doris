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

import org.apache.hudi.HoodieBaseRelation.convertToAvroSchema
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.{DataSourceReadOptions, HoodieMergeOnReadFileSplit, HoodieTableSchema}
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Reference to Apache Hudi
 * see <a href="https://github.com/apache/hudi/blob/release-0.13.0/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/MergeOnReadSnapshotRelation.scala">MergeOnReadSnapshotRelation</a>
 */
class MORSnapshotSplitReader(override val split: HoodieSplit) extends BaseSplitReader(split) {
  /**
   * NOTE: These are the fields that are required to properly fulfil Merge-on-Read (MOR)
   * semantic:
   *
   * <ol>
   * <li>Primary key is required to make sure we're able to correlate records from the base
   * file with the updated records from the delta-log file</li>
   * <li>Pre-combine key is required to properly perform the combining (or merging) of the
   * existing and updated records</li>
   * </ol>
   *
   * However, in cases when merging is NOT performed (for ex, if file-group only contains base
   * files but no delta-log files, or if the query-type is equal to [["skip_merge"]]) neither
   * of primary-key or pre-combine-key are required to be fetched from storage (unless requested
   * by the query), therefore saving on throughput
   */
  protected lazy val mandatoryFieldsForMerging: Seq[String] =
    Seq(recordKeyField) ++ preCombineFieldOpt.map(Seq(_)).getOrElse(Seq())

  override lazy val mandatoryFields: Seq[String] = mandatoryFieldsForMerging

  protected val mergeType: String = optParams.getOrElse(DataSourceReadOptions.REALTIME_MERGE.key,
    DataSourceReadOptions.REALTIME_MERGE.defaultValue)

  override protected def composeIterator(tableSchema: HoodieTableSchema,
                                         requiredSchema: HoodieTableSchema,
                                         requestedColumns: Array[String],
                                         filters: Array[Filter]): Iterator[InternalRow] = {
    // todo: push down predicates about key field
    val requiredFilters = Seq.empty
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)

    new HoodieMORRecordIterator(split.hadoopConf,
      readers,
      tableSchema,
      requiredSchema,
      tableState,
      mergeType,
      getFileSplit())
  }

  protected def getFileSplit(): HoodieMergeOnReadFileSplit = {
    val logFiles = split.deltaFilePaths.map(new HoodieLogFile(_))
      .sorted(Ordering.comparatorToOrdering(HoodieLogFile.getLogFileComparator)).toList
    val partitionedBaseFile = if (split.dataFilePath.isEmpty) {
      None
    } else {
      Some(PartitionedFile(getPartitionColumnsAsInternalRow(), SparkPath.fromPathString(split.dataFilePath), 0, split.dataFileLength))
    }
    HoodieMergeOnReadFileSplit(partitionedBaseFile, logFiles)
  }

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "true")
    // there's no thread local TaskContext, so the parquet reader will still use on heap memory even setting true
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.columnVector.offheap.enabled", "true")
  }

  protected def createBaseFileReaders(tableSchema: HoodieTableSchema,
                                      requiredSchema: HoodieTableSchema,
                                      requestedColumns: Array[String],
                                      requiredFilters: Seq[Filter],
                                      optionalFilters: Seq[Filter] = Seq.empty): HoodieMergeOnReadBaseFileReaders = {
    val (partitionSchema, dataSchema, requiredDataSchema) =
      tryPrunePartitionColumns(tableSchema, requiredSchema)

    val fullSchemaReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = dataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly);
      // As such only required filters could be pushed-down to such reader
      filters = requiredFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(split.hadoopConf, internalSchemaOpt)
    )

    val requiredSchemaReader = createBaseFileReader(
      spark = sqlContext.sparkSession,
      partitionSchema = partitionSchema,
      dataSchema = dataSchema,
      requiredDataSchema = requiredDataSchema,
      // This file-reader is used to read base file records, subsequently merging them with the records
      // stored in delta-log files. As such, we have to read _all_ records from the base file, while avoiding
      // applying any filtering _before_ we complete combining them w/ delta-log records (to make sure that
      // we combine them correctly);
      // As such only required filters could be pushed-down to such reader
      filters = requiredFilters,
      options = optParams,
      // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
      //       to configure Parquet reader appropriately
      hadoopConf = embedInternalSchema(split.hadoopConf, requiredDataSchema.internalSchema)
    )

    // Check whether fields required for merging were also requested to be fetched
    // by the query:
    //    - In case they were, there's no optimization we could apply here (we will have
    //    to fetch such fields)
    //    - In case they were not, we will provide 2 separate file-readers
    //        a) One which would be applied to file-groups w/ delta-logs (merging)
    //        b) One which would be applied to file-groups w/ no delta-logs or
    //           in case query-mode is skipping merging
    val mandatoryColumns = mandatoryFieldsForMerging.map(HoodieAvroUtils.getRootLevelFieldName)
    if (mandatoryColumns.forall(requestedColumns.contains)) {
      HoodieMergeOnReadBaseFileReaders(
        fullSchemaReader = fullSchemaReader,
        requiredSchemaReader = requiredSchemaReader,
        requiredSchemaReaderSkipMerging = requiredSchemaReader
      )
    } else {
      val prunedRequiredSchema = {
        val unusedMandatoryColumnNames = mandatoryColumns.filterNot(requestedColumns.contains)
        val prunedStructSchema =
          StructType(requiredDataSchema.structTypeSchema.fields
            .filterNot(f => unusedMandatoryColumnNames.contains(f.name)))

        HoodieTableSchema(prunedStructSchema, convertToAvroSchema(prunedStructSchema, tableName).toString)
      }

      val requiredSchemaReaderSkipMerging = createBaseFileReader(
        spark = sqlContext.sparkSession,
        partitionSchema = partitionSchema,
        dataSchema = dataSchema,
        requiredDataSchema = prunedRequiredSchema,
        // This file-reader is only used in cases when no merging is performed, therefore it's safe to push
        // down these filters to the base file readers
        filters = requiredFilters ++ optionalFilters,
        options = optParams,
        // NOTE: We have to fork the Hadoop Config here as Spark will be modifying it
        //       to configure Parquet reader appropriately
        hadoopConf = embedInternalSchema(split.hadoopConf, requiredDataSchema.internalSchema)
      )

      HoodieMergeOnReadBaseFileReaders(
        fullSchemaReader = fullSchemaReader,
        requiredSchemaReader = requiredSchemaReader,
        requiredSchemaReaderSkipMerging = requiredSchemaReaderSkipMerging
      )
    }
  }
}
