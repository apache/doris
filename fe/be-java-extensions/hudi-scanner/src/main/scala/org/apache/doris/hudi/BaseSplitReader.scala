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

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.doris.common.jni.vec.ColumnType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.io.hfile.CacheConfig
import org.apache.hudi.HoodieBaseRelation.{BaseFileReader, convertToAvroSchema}
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.{ConfigProperty, HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.{ConfigUtils, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.CachingPath
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter
import org.apache.hudi.internal.schema.utils.{InternalSchemaUtils, SerDeHelper}
import org.apache.hudi.internal.schema.{HoodieSchemaException, InternalSchema}
import org.apache.hudi.io.storage.HoodieAvroHFileReader
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceWriteOptions, HoodieSparkConfUtils, HoodieTableSchema, HoodieTableState}
import org.apache.log4j.Logger
import org.apache.spark.sql.adapter.Spark3_4Adapter
import org.apache.spark.sql.avro.{HoodieAvroSchemaConverters, HoodieSparkAvroSchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{PartitionedFile, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.{SQLContext, SparkSession, SparkSessionExtensions}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{SparkConf, SparkContext}

import java.lang.reflect.Constructor
import java.net.URI
import java.util.Objects
import java.util.concurrent.TimeUnit
import java.{util => jutil}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

class DorisSparkAdapter extends Spark3_4Adapter {
  override def getAvroSchemaConverters: HoodieAvroSchemaConverters = HoodieSparkAvroSchemaConverters
}

class HoodieSplit(private val params: jutil.Map[String, String]) {
  val queryId: String = params.remove("query_id")
  val basePath: String = params.remove("base_path")
  val dataFilePath: String = params.remove("data_file_path")
  val dataFileLength: Long = params.remove("data_file_length").toLong
  val deltaFilePaths: Array[String] = {
    val deltas = params.remove("delta_file_paths")
    if (StringUtils.isNullOrEmpty(deltas)) new Array[String](0) else deltas.split(",")
  }

  val hudiColumnNames: Array[String] = params.remove("hudi_column_names").split(",")
  val hudiColumnTypes: Map[String, String] = hudiColumnNames.zip(
    params.remove("hudi_column_types").split("#")).toMap

  val requiredFields: Array[String] = {
    val readFields = params.remove("required_fields").split(",").filter(_.nonEmpty)
    if (readFields.isEmpty) {
      // If only read the partition columns, the JniConnector will produce empty required fields.
      // Read the "_hoodie_record_key" field at least to know how many rows in current hoodie split
      // Even if the JniConnector doesn't read this field, the call of releaseTable will reclaim the resource
      Array(HoodieRecord.RECORD_KEY_METADATA_FIELD)
    } else {
      readFields
    }
  }
  val requiredTypes: Array[ColumnType] = requiredFields.map(
    field => ColumnType.parseType(field, hudiColumnTypes(field)))

  val nestedFields: Array[String] = {
    val fields = params.remove("nested_fields")
    if (StringUtils.isNullOrEmpty(fields)) new Array[String](0) else fields.split(",")
  }
  val instantTime: String = params.remove("instant_time")
  val serde: String = params.remove("serde")
  val inputFormat: String = params.remove("input_format")

  val hadoopProperties: Map[String, String] = {
    val properties = new jutil.HashMap[String, String]
    val iterator = params.entrySet().iterator()
    while (iterator.hasNext) {
      val kv = iterator.next()
      if (kv.getKey.startsWith(BaseSplitReader.HADOOP_CONF_PREFIX)) {
        properties.put(kv.getKey.substring(BaseSplitReader.HADOOP_CONF_PREFIX.length), kv.getValue)
        iterator.remove()
      }
    }
    properties.asScala.toMap
  }

  lazy val hadoopConf: Configuration = {
    val conf = new Configuration
    hadoopProperties.foreach(kv => conf.set(kv._1, kv._2))
    conf
  }

  def incrementalRead: Boolean = {
    "true".equalsIgnoreCase(optParams.getOrElse("hoodie.datasource.read.incr.operation", "false"))
  }

  // NOTE: In cases when Hive Metastore is used as catalog and the table is partitioned, schema in the HMS might contain
  //       Hive-specific partitioning columns created specifically for HMS to handle partitioning appropriately. In that
  //       case  we opt in to not be providing catalog's schema, and instead force Hudi relations to fetch the schema
  //       from the table itself
  val schemaSpec: Option[StructType] = None

  val optParams: Map[String, String] = params.asScala.toMap

  override def equals(obj: Any): Boolean = {
    if (obj == null) {
      return false
    }
    obj match {
      case split: HoodieSplit =>
        hashCode() == split.hashCode()
      case _ => false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(queryId, basePath)
  }
}

case class HoodieTableInformation(sparkSession: SparkSession,
                                  metaClient: HoodieTableMetaClient,
                                  timeline: HoodieTimeline,
                                  tableConfig: HoodieTableConfig,
                                  resolvedTargetFields: Array[String],
                                  tableAvroSchema: Schema,
                                  internalSchemaOpt: Option[InternalSchema])

/**
 * Reference to Apache Hudi
 * see <a href="https://github.com/apache/hudi/blob/release-0.13.0/hudi-spark-datasource/hudi-spark-common/src/main/scala/org/apache/hudi/HoodieBaseRelation.scala">HoodieBaseRelation</a>
 */
abstract class BaseSplitReader(val split: HoodieSplit) {

  import BaseSplitReader._

  protected val optParams: Map[String, String] = split.optParams

  protected val tableInformation: HoodieTableInformation = cache.get(split)

  protected val timeline: HoodieTimeline = tableInformation.timeline

  protected val sparkSession: SparkSession = tableInformation.sparkSession
  protected val sqlContext: SQLContext = sparkSession.sqlContext
  imbueConfigs(sqlContext)

  protected val tableConfig: HoodieTableConfig = tableInformation.tableConfig
  protected val tableName: String = tableConfig.getTableName

  // NOTE: Record key-field is assumed singular here due to the either of
  //          - In case Hudi's meta fields are enabled: record key will be pre-materialized (stored) as part
  //          of the record's payload (as part of the Hudi's metadata)
  //          - In case Hudi's meta fields are disabled (virtual keys): in that case record has to bear _single field_
  //          identified as its (unique) primary key w/in its payload (this is a limitation of [[SimpleKeyGenerator]],
  //          which is the only [[KeyGenerator]] permitted for virtual-keys payloads)
  protected lazy val recordKeyField: String =
  if (tableConfig.populateMetaFields()) {
    HoodieRecord.RECORD_KEY_METADATA_FIELD
  } else {
    val keyFields = tableConfig.getRecordKeyFields.get()
    checkState(keyFields.length == 1)
    keyFields.head
  }

  protected lazy val preCombineFieldOpt: Option[String] =
    Option(tableConfig.getPreCombineField)
      .orElse(optParams.get(DataSourceWriteOptions.PRECOMBINE_FIELD.key)) match {
      // NOTE: This is required to compensate for cases when empty string is used to stub
      //       property value to avoid it being set with the default value
      // TODO(HUDI-3456) cleanup
      case Some(f) if !StringUtils.isNullOrEmpty(f) => Some(f)
      case _ => None
    }

  /**
   * Columns that relation has to read from the storage to properly execute on its semantic: for ex,
   * for Merge-on-Read tables key fields as well and pre-combine field comprise mandatory set of columns,
   * meaning that regardless of whether this columns are being requested by the query they will be fetched
   * regardless so that relation is able to combine records properly (if necessary)
   *
   * @VisibleInTests
   */
  val mandatoryFields: Seq[String]

  /**
   * NOTE: Initialization of teh following members is coupled on purpose to minimize amount of I/O
   * required to fetch table's Avro and Internal schemas
   */
  protected lazy val (tableAvroSchema: Schema, internalSchemaOpt: Option[InternalSchema]) = {
    (tableInformation.tableAvroSchema, tableInformation.internalSchemaOpt)
  }

  protected lazy val tableStructSchema: StructType = convertAvroSchemaToStructType(tableAvroSchema)

  protected lazy val partitionColumns: Array[String] = tableConfig.getPartitionFields.orElse(Array.empty)

  protected lazy val specifiedQueryTimestamp: Option[String] = Some(split.instantTime)

  private def queryTimestamp: Option[String] =
    specifiedQueryTimestamp.orElse(toScalaOption(timeline.lastInstant()).map(_.getTimestamp))

  lazy val tableState: HoodieTableState = {
    val recordMergerImpls = ConfigUtils.split2List(getConfigValue(HoodieWriteConfig.RECORD_MERGER_IMPLS)).asScala.toList
    val recordMergerStrategy = getConfigValue(HoodieWriteConfig.RECORD_MERGER_STRATEGY,
      Option(tableInformation.metaClient.getTableConfig.getRecordMergerStrategy))
    val configProperties = getConfigProperties(sparkSession, optParams)
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(configProperties)
      .enable(configProperties.getBoolean(
        HoodieMetadataConfig.ENABLE.key(), HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS)
        && HoodieTableMetadataUtil.isFilesPartitionAvailable(tableInformation.metaClient))
      .build()

    // Subset of the state of table's configuration as of at the time of the query
    HoodieTableState(
      tablePath = split.basePath,
      latestCommitTimestamp = queryTimestamp,
      recordKeyField = recordKeyField,
      preCombineFieldOpt = preCombineFieldOpt,
      usesVirtualKeys = !tableConfig.populateMetaFields(),
      recordPayloadClassName = tableConfig.getPayloadClass,
      metadataConfig = metadataConfig,
      recordMergerImpls = recordMergerImpls,
      recordMergerStrategy = recordMergerStrategy
    )
  }

  private def getConfigValue(config: ConfigProperty[String],
                             defaultValueOption: Option[String] = Option.empty): String = {
    optParams.getOrElse(config.key(),
      sqlContext.getConf(config.key(), defaultValueOption.getOrElse(config.defaultValue())))
  }

  def imbueConfigs(sqlContext: SQLContext): Unit = {
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.recordLevelFilter.enabled", "true")
    // TODO(HUDI-3639) vectorized reader has to be disabled to make sure MORIncrementalRelation is working properly
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }

  def buildScanIterator(filters: Array[Filter]): Iterator[InternalRow] = {
    // NOTE: PLEASE READ CAREFULLY BEFORE MAKING CHANGES
    //       *Appending* additional columns to the ones requested by the caller is not a problem, as those
    //       will be eliminated by the caller's projection;
    //   (!) Please note, however, that it's critical to avoid _reordering_ of the requested columns as this
    //       will break the upstream projection
    val targetColumns: Array[String] = appendMandatoryColumns(tableInformation.resolvedTargetFields)
    // NOTE: We explicitly fallback to default table's Avro schema to make sure we avoid unnecessary Catalyst > Avro
    //       schema conversion, which is lossy in nature (for ex, it doesn't preserve original Avro type-names) and
    //       could have an effect on subsequent de-/serializing records in some exotic scenarios (when Avro unions
    //       w/ more than 2 types are involved)
    val sourceSchema = tableAvroSchema
    val (requiredAvroSchema, requiredStructSchema, requiredInternalSchema) =
      projectSchema(Either.cond(internalSchemaOpt.isDefined, internalSchemaOpt.get, sourceSchema), targetColumns)

    val tableAvroSchemaStr = tableAvroSchema.toString
    val tableSchema = HoodieTableSchema(tableStructSchema, tableAvroSchemaStr, internalSchemaOpt)
    val requiredSchema = HoodieTableSchema(
      requiredStructSchema, requiredAvroSchema.toString, Some(requiredInternalSchema))

    composeIterator(tableSchema, requiredSchema, targetColumns, filters)
  }

  /**
   * Composes iterator provided file split to read from, table and partition schemas, data filters to be applied
   *
   * @param tableSchema      target table's schema
   * @param requiredSchema   projected schema required by the reader
   * @param requestedColumns columns requested by the query
   * @param filters          data filters to be applied
   * @return instance of RDD (holding [[InternalRow]]s)
   */
  protected def composeIterator(tableSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                requestedColumns: Array[String],
                                filters: Array[Filter]): Iterator[InternalRow]

  private final def appendMandatoryColumns(requestedColumns: Array[String]): Array[String] = {
    // For a nested field in mandatory columns, we should first get the root-level field, and then
    // check for any missing column, as the requestedColumns should only contain root-level fields
    // We should only append root-level field as well
    val missing = mandatoryFields.map(col => HoodieAvroUtils.getRootLevelFieldName(col))
      .filter(rootField => !requestedColumns.contains(rootField))
    requestedColumns ++ missing
  }

  /**
   * Projects provided schema by picking only required (projected) top-level columns from it
   *
   * @param tableSchema     schema to project (either of [[InternalSchema]] or Avro's [[Schema]])
   * @param requiredColumns required top-level columns to be projected
   */
  def projectSchema(tableSchema: Either[Schema, InternalSchema],
                    requiredColumns: Array[String]): (Schema, StructType, InternalSchema) = {
    tableSchema match {
      case Right(internalSchema) =>
        checkState(!internalSchema.isEmptySchema)
        val prunedInternalSchema = InternalSchemaUtils.pruneInternalSchema(
          internalSchema, requiredColumns.toList.asJava)
        val requiredAvroSchema = AvroInternalSchemaConverter.convert(prunedInternalSchema, "schema")
        val requiredStructSchema = convertAvroSchemaToStructType(requiredAvroSchema)

        (requiredAvroSchema, requiredStructSchema, prunedInternalSchema)

      case Left(avroSchema) =>
        val fieldMap = avroSchema.getFields.asScala.map(f => f.name() -> f).toMap
        val requiredFields = requiredColumns.map { col =>
          val f = fieldMap(col)
          // We have to create a new [[Schema.Field]] since Avro schemas can't share field
          // instances (and will throw "org.apache.avro.AvroRuntimeException: Field already used")
          new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultVal(), f.order())
        }.toList
        val requiredAvroSchema = Schema.createRecord(avroSchema.getName, avroSchema.getDoc,
          avroSchema.getNamespace, avroSchema.isError, requiredFields.asJava)
        val requiredStructSchema = convertAvroSchemaToStructType(requiredAvroSchema)

        (requiredAvroSchema, requiredStructSchema, InternalSchema.getEmptyInternalSchema)
    }
  }

  /**
   * Converts Avro's [[Schema]] to Catalyst's [[StructType]]
   */
  protected def convertAvroSchemaToStructType(avroSchema: Schema): StructType = {
    val schemaConverters = sparkAdapter.getAvroSchemaConverters
    schemaConverters.toSqlType(avroSchema) match {
      case (dataType, _) => dataType.asInstanceOf[StructType]
    }
  }

  protected def tryPrunePartitionColumns(tableSchema: HoodieTableSchema,
                                         requiredSchema: HoodieTableSchema): (StructType, HoodieTableSchema, HoodieTableSchema) = {
    // Since schema requested by the caller might contain partition columns, we might need to
    // prune it, removing all partition columns from it in case these columns are not persisted
    // in the data files
    //
    // NOTE: This partition schema is only relevant to file reader to be able to embed
    //       values of partition columns (hereafter referred to as partition values) encoded into
    //       the partition path, and omitted from the data file, back into fetched rows;
    //       Note that, by default, partition columns are not omitted therefore specifying
    //       partition schema for reader is not required
    if (shouldExtractPartitionValuesFromPartitionPath) {
      val partitionSchema = StructType(partitionColumns.map(StructField(_, StringType)))
      val prunedDataStructSchema = prunePartitionColumns(tableSchema.structTypeSchema)
      val prunedRequiredSchema = prunePartitionColumns(requiredSchema.structTypeSchema)

      (partitionSchema,
        HoodieTableSchema(prunedDataStructSchema, convertToAvroSchema(prunedDataStructSchema, tableName).toString),
        HoodieTableSchema(prunedRequiredSchema, convertToAvroSchema(prunedRequiredSchema, tableName).toString))
    } else {
      (StructType(Nil), tableSchema, requiredSchema)
    }
  }

  /**
   * Controls whether partition values (ie values of partition columns) should be
   * <ol>
   * <li>Extracted from partition path and appended to individual rows read from the data file (we
   * delegate this to Spark's [[ParquetFileFormat]])</li>
   * <li>Read from the data-file as is (by default Hudi persists all columns including partition ones)</li>
   * </ol>
   *
   * This flag is only be relevant in conjunction with the usage of [["hoodie.datasource.write.drop.partition.columns"]]
   * config, when Hudi will NOT be persisting partition columns in the data file, and therefore values for
   * such partition columns (ie "partition values") will have to be parsed from the partition path, and appended
   * to every row only in the fetched dataset.
   *
   * NOTE: Partition values extracted from partition path might be deviating from the values of the original
   * partition columns: for ex, if originally as partition column was used column [[ts]] bearing epoch
   * timestamp, which was used by [[TimestampBasedKeyGenerator]] to generate partition path of the format
   * [["yyyy/mm/dd"]], appended partition value would bear the format verbatim as it was used in the
   * partition path, meaning that string value of "2022/01/01" will be appended, and not its original
   * representation
   */
  protected val shouldExtractPartitionValuesFromPartitionPath: Boolean = {
    // Controls whether partition columns (which are the source for the partition path values) should
    // be omitted from persistence in the data files. On the read path it affects whether partition values (values
    // of partition columns) will be read from the data file or extracted from partition path

    val shouldOmitPartitionColumns = tableInformation.tableConfig.shouldDropPartitionColumns && partitionColumns.nonEmpty
    val shouldExtractPartitionValueFromPath =
      optParams.getOrElse(DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.key,
        DataSourceReadOptions.EXTRACT_PARTITION_VALUES_FROM_PARTITION_PATH.defaultValue.toString).toBoolean
    shouldOmitPartitionColumns || shouldExtractPartitionValueFromPath
  }

  private def prunePartitionColumns(dataStructSchema: StructType): StructType =
    StructType(dataStructSchema.filterNot(f => partitionColumns.contains(f.name)))

  /**
   * For enable hoodie.datasource.write.drop.partition.columns, need to create an InternalRow on partition values
   * and pass this reader on parquet file. So that, we can query the partition columns.
   */
  protected def getPartitionColumnsAsInternalRow(): InternalRow = {
    try {
      if (shouldExtractPartitionValuesFromPartitionPath) {
        val filePath = new Path(split.dataFilePath)
        val tablePathWithoutScheme = CachingPath.getPathWithoutSchemeAndAuthority(tableInformation.metaClient.getBasePathV2)
        val partitionPathWithoutScheme = CachingPath.getPathWithoutSchemeAndAuthority(filePath.getParent)
        val relativePath = new URI(tablePathWithoutScheme.toString).relativize(new URI(partitionPathWithoutScheme.toString)).toString
        val hiveStylePartitioningEnabled = tableConfig.getHiveStylePartitioningEnable.toBoolean
        if (hiveStylePartitioningEnabled) {
          val partitionSpec = PartitioningUtils.parsePathFragment(relativePath)
          InternalRow.fromSeq(partitionColumns.map(partitionSpec(_)).map(UTF8String.fromString))
        } else {
          if (partitionColumns.length == 1) {
            InternalRow.fromSeq(Seq(UTF8String.fromString(relativePath)))
          } else {
            val parts = relativePath.split("/")
            assert(parts.size == partitionColumns.length)
            InternalRow.fromSeq(parts.map(UTF8String.fromString))
          }
        }
      } else {
        InternalRow.empty
      }
    } catch {
      case NonFatal(e) =>
        LOG.warn(s"Failed to get the right partition InternalRow for file: ${split.dataFilePath}", e)
        InternalRow.empty
    }
  }

  /**
   * Wrapper for `buildReaderWithPartitionValues` of [[ParquetFileFormat]] handling [[ColumnarBatch]],
   * when Parquet's Vectorized Reader is used
   *
   * TODO move to HoodieBaseRelation, make private
   */
  private[hudi] def buildHoodieParquetReader(sparkSession: SparkSession,
                                             dataSchema: StructType,
                                             partitionSchema: StructType,
                                             requiredSchema: StructType,
                                             filters: Seq[Filter],
                                             options: Map[String, String],
                                             hadoopConf: Configuration,
                                             appendPartitionValues: Boolean = false): PartitionedFile => Iterator[InternalRow] = {
    val parquetFileFormat: ParquetFileFormat = sparkAdapter.createLegacyHoodieParquetFileFormat(appendPartitionValues).get
    val readParquetFile: PartitionedFile => Iterator[Any] = parquetFileFormat.buildReaderWithPartitionValues(
      sparkSession = sparkSession,
      dataSchema = dataSchema,
      partitionSchema = partitionSchema,
      requiredSchema = requiredSchema,
      filters = filters,
      options = options,
      hadoopConf = hadoopConf
    )

    file: PartitionedFile => {
      val iter = readParquetFile(file)
      iter.flatMap {
        case r: InternalRow => Seq(r)
        case b: ColumnarBatch => b.rowIterator().asScala
      }
    }
  }

  private def createHFileReader(spark: SparkSession,
                                dataSchema: HoodieTableSchema,
                                requiredDataSchema: HoodieTableSchema,
                                filters: Seq[Filter],
                                options: Map[String, String],
                                hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    partitionedFile => {
      val reader = new HoodieAvroHFileReader(
        hadoopConf, partitionedFile.filePath.toPath, new CacheConfig(hadoopConf))

      val requiredRowSchema = requiredDataSchema.structTypeSchema
      // NOTE: Schema has to be parsed at this point, since Avro's [[Schema]] aren't serializable
      //       to be passed from driver to executor
      val requiredAvroSchema = new Schema.Parser().parse(requiredDataSchema.avroSchemaStr)
      val avroToRowConverter = AvroConversionUtils.createAvroToInternalRowConverter(
        requiredAvroSchema, requiredRowSchema)

      reader.getRecordIterator(requiredAvroSchema).asScala
        .map(record => {
          avroToRowConverter.apply(record.getData.asInstanceOf[GenericRecord]).get
        })
    }
  }

  /**
   * Returns file-reader routine accepting [[PartitionedFile]] and returning an [[Iterator]]
   * over [[InternalRow]]
   */
  protected def createBaseFileReader(spark: SparkSession,
                                     partitionSchema: StructType,
                                     dataSchema: HoodieTableSchema,
                                     requiredDataSchema: HoodieTableSchema,
                                     filters: Seq[Filter],
                                     options: Map[String, String],
                                     hadoopConf: Configuration): BaseFileReader = {
    val tableBaseFileFormat = tableConfig.getBaseFileFormat

    // NOTE: PLEASE READ CAREFULLY
    //       Lambda returned from this method is going to be invoked on the executor, and therefore
    //       we have to eagerly initialize all of the readers even though only one specific to the type
    //       of the file being read will be used. This is required to avoid serialization of the whole
    //       relation (containing file-index for ex) and passing it to the executor
    val (read: (PartitionedFile => Iterator[InternalRow]), schema: StructType) =
    tableBaseFileFormat match {
      case HoodieFileFormat.PARQUET =>
        val parquetReader = buildHoodieParquetReader(
          sparkSession = spark,
          dataSchema = dataSchema.structTypeSchema,
          partitionSchema = partitionSchema,
          requiredSchema = requiredDataSchema.structTypeSchema,
          filters = filters,
          options = options,
          hadoopConf = hadoopConf,
          // We're delegating to Spark to append partition values to every row only in cases
          // when these corresponding partition-values are not persisted w/in the data file itself
          appendPartitionValues = shouldExtractPartitionValuesFromPartitionPath
        )
        // Since partition values by default are omitted, and not persisted w/in data-files by Spark,
        // data-file readers (such as [[ParquetFileFormat]]) have to inject partition values while reading
        // the data. As such, actual full schema produced by such reader is composed of
        //    a) Data-file schema (projected or not)
        //    b) Appended partition column values
        val readerSchema = StructType(requiredDataSchema.structTypeSchema.fields ++ partitionSchema.fields)

        (parquetReader, readerSchema)

      case HoodieFileFormat.HFILE =>
        val hfileReader = createHFileReader(
          spark = spark,
          dataSchema = dataSchema,
          requiredDataSchema = requiredDataSchema,
          filters = filters,
          options = options,
          hadoopConf = hadoopConf
        )

        (hfileReader, requiredDataSchema.structTypeSchema)

      case _ => throw new UnsupportedOperationException(s"Base file format is not currently supported ($tableBaseFileFormat)")
    }

    BaseFileReader(
      read = partitionedFile => {
        val extension = FSUtils.getFileExtension(partitionedFile.filePath.toString())
        if (tableBaseFileFormat.getFileExtension.equals(extension)) {
          read(partitionedFile)
        } else {
          throw new UnsupportedOperationException(s"Invalid base-file format ($extension), expected ($tableBaseFileFormat)")
        }
      },
      schema = schema
    )
  }

  protected def embedInternalSchema(conf: Configuration, internalSchemaOpt: Option[InternalSchema]): Configuration = {
    val internalSchema = internalSchemaOpt.getOrElse(InternalSchema.getEmptyInternalSchema)
    val querySchemaString = SerDeHelper.toJson(internalSchema)
    if (!StringUtils.isNullOrEmpty(querySchemaString)) {
      val validCommits = timeline.getInstants.iterator.asScala.map(_.getFileName).mkString(",")
      LOG.warn(s"Table valid commits: $validCommits")

      conf.set(SparkInternalSchemaConverter.HOODIE_QUERY_SCHEMA, SerDeHelper.toJson(internalSchema))
      conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, split.basePath)
      conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
    }
    conf
  }
}

object SparkMockHelper {
  private lazy val mockSparkContext = {
    val conf = new SparkConf().setMaster("local").setAppName("mock_sc")
      .set("spark.ui.enabled", "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  implicit class MockSparkSession(builder: SparkSession.Builder) {
    def createMockSession(split: HoodieSplit): SparkSession = {
      val sparkSessionClass = classOf[SparkSession]
      val constructor: Constructor[SparkSession] = sparkSessionClass.getDeclaredConstructors
        .find(_.getParameterCount == 5).get.asInstanceOf[Constructor[SparkSession]]
      constructor.setAccessible(true)
      val ss = constructor.newInstance(mockSparkContext, None, None, new SparkSessionExtensions, Map.empty)
      split.hadoopProperties.foreach(kv => ss.sessionState.conf.setConfString(kv._1, kv._2))
      ss
    }
  }
}

object BaseSplitReader {

  import SparkMockHelper.MockSparkSession

  private val LOG = Logger.getLogger(BaseSplitReader.getClass)
  val HADOOP_CONF_PREFIX = "hadoop_conf."

  // Use [[SparkAdapterSupport]] instead ?
  private lazy val sparkAdapter = new DorisSparkAdapter

  private lazy val cache: LoadingCache[HoodieSplit, HoodieTableInformation] = {
    val loader = new CacheLoader[HoodieSplit, HoodieTableInformation] {
      override def load(split: HoodieSplit): HoodieTableInformation = {
        // create mock spark session
        val sparkSession = SparkSession.builder().createMockSession(split)
        val metaClient = Utils.getMetaClient(split.hadoopConf, split.basePath)
        // NOTE: We're including compaction here since it's not considering a "commit" operation
        val timeline = metaClient.getCommitsAndCompactionTimeline.filterCompletedInstants

        val specifiedQueryTimestamp: Option[String] = Some(split.instantTime)
        val schemaResolver = new TableSchemaResolver(metaClient)
        val internalSchemaOpt = if (!isSchemaEvolutionEnabledOnRead(split.optParams, sparkSession)) {
          None
        } else {
          Try {
            specifiedQueryTimestamp.map(schemaResolver.getTableInternalSchemaFromCommitMetadata)
              .getOrElse(schemaResolver.getTableInternalSchemaFromCommitMetadata)
          } match {
            case Success(internalSchemaOpt) => toScalaOption(internalSchemaOpt)
            case Failure(_) =>
              None
          }
        }
        val tableName = metaClient.getTableConfig.getTableName
        val (name, namespace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
        val avroSchema: Schema = internalSchemaOpt.map { is =>
          AvroInternalSchemaConverter.convert(is, namespace + "." + name)
        } orElse {
          specifiedQueryTimestamp.map(schemaResolver.getTableAvroSchema)
        } orElse {
          split.schemaSpec.map(s => convertToAvroSchema(s, tableName))
        } getOrElse {
          Try(schemaResolver.getTableAvroSchema) match {
            case Success(schema) => schema
            case Failure(e) =>
              throw new HoodieSchemaException("Failed to fetch schema from the table", e)
          }
        }

        // match column name in lower case
        val colNames = internalSchemaOpt.map { internalSchema =>
          internalSchema.getAllColsFullName.asScala.map(f => f.toLowerCase -> f).toMap
        } getOrElse {
          avroSchema.getFields.asScala.map(f => f.name().toLowerCase -> f.name()).toMap
        }
        val resolvedTargetFields = split.requiredFields.map(field => colNames.getOrElse(field.toLowerCase, field))

        HoodieTableInformation(sparkSession,
          metaClient,
          timeline,
          metaClient.getTableConfig,
          resolvedTargetFields,
          avroSchema,
          internalSchemaOpt)
      }
    }
    CacheBuilder.newBuilder()
      .expireAfterAccess(10, TimeUnit.MINUTES)
      .maximumSize(4096)
      .build(loader)
  }

  private def isSchemaEvolutionEnabledOnRead(optParams: Map[String, String], sparkSession: SparkSession): Boolean = {
    // NOTE: Schema evolution could be configured both t/h optional parameters vehicle as well as
    //       t/h Spark Session configuration (for ex, for Spark SQL)
    optParams.getOrElse(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean ||
      sparkSession.sessionState.conf.getConfString(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.key,
        DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue.toString).toBoolean
  }

  private def getConfigProperties(spark: SparkSession, options: Map[String, String]) = {
    val sqlConf: SQLConf = spark.sessionState.conf
    val properties = new TypedProperties()
    // Ambiguous reference when invoking Properties.putAll() in Java 11
    // Reference https://github.com/scala/bug/issues/10418
    options.filter(p => p._2 != null).foreach(p => properties.setProperty(p._1, p._2))

    // TODO(HUDI-5361) clean up properties carry-over

    // To support metadata listing via Spark SQL we allow users to pass the config via SQL Conf in spark session. Users
    // would be able to run SET hoodie.metadata.enable=true in the spark sql session to enable metadata listing.
    val isMetadataTableEnabled = HoodieSparkConfUtils.getConfigValue(options, sqlConf, HoodieMetadataConfig.ENABLE.key, null)
    if (isMetadataTableEnabled != null) {
      properties.setProperty(HoodieMetadataConfig.ENABLE.key(), String.valueOf(isMetadataTableEnabled))
    }

    val listingModeOverride = HoodieSparkConfUtils.getConfigValue(options, sqlConf,
      DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key, null)
    if (listingModeOverride != null) {
      properties.setProperty(DataSourceReadOptions.FILE_INDEX_LISTING_MODE_OVERRIDE.key, listingModeOverride)
    }

    properties
  }
}
