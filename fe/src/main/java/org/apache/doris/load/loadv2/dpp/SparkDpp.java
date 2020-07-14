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

package org.apache.doris.load.loadv2.dpp;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.common.UserException;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.TaskContext;

import scala.Tuple2;
import scala.collection.Seq;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import scala.collection.JavaConverters;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

// This class is a Spark-based data preprocessing program,
// which will make use of the distributed compute framework of spark to
// do ETL job/sort/preaggregate jobs in spark job
// to boost the process of large amount of data load.
// the process steps are as following:
// 1. load data
//     1.1 load data from path/hive table
//     1.2 do the etl process
// 2. repartition data by using doris data model(partition and bucket)
// 3. process aggregation if needed
// 4. write data to parquet file
public final class SparkDpp implements java.io.Serializable {
    private static final Logger LOG = LogManager.getLogger(SparkDpp.class);

    private static final String NULL_FLAG = "\\N";
    private static final String DPP_RESULT_FILE = "dpp_result.json";
    private static final String BITMAP_TYPE = "bitmap";
    private SparkSession spark = null;
    private EtlJobConfig etlJobConfig = null;
    private LongAccumulator abnormalRowAcc = null;
    private LongAccumulator unselectedRowAcc = null;
    private LongAccumulator scannedRowsAcc = null;
    private LongAccumulator fileNumberAcc = null;
    private LongAccumulator fileSizeAcc = null;
    private Map<String, Integer> bucketKeyMap = new HashMap<>();
    // accumulator to collect invalid rows
    private StringAccumulator invalidRows = new StringAccumulator();

    public SparkDpp(SparkSession spark, EtlJobConfig etlJobConfig) {
        this.spark = spark;
        this.etlJobConfig = etlJobConfig;
    }

    public void init() {
        abnormalRowAcc = spark.sparkContext().longAccumulator("abnormalRowAcc");
        unselectedRowAcc = spark.sparkContext().longAccumulator("unselectedRowAcc");
        scannedRowsAcc = spark.sparkContext().longAccumulator("scannedRowsAcc");
        fileNumberAcc = spark.sparkContext().longAccumulator("fileNumberAcc");
        fileSizeAcc = spark.sparkContext().longAccumulator("fileSizeAcc");
        spark.sparkContext().register(invalidRows, "InvalidRowsAccumulator");
    }

    private Dataset<Row> processRDDAggAndRepartition(Dataset<Row> dataframe, EtlJobConfig.EtlIndex currentIndexMeta) throws UserException {
        final boolean isDuplicateTable = !StringUtils.equalsIgnoreCase(currentIndexMeta.indexType, "AGGREGATE")
                && !StringUtils.equalsIgnoreCase(currentIndexMeta.indexType, "UNIQUE");

        // 1 make metadata for map/reduce
        int keyLen = 0;
        for (EtlJobConfig.EtlColumn etlColumn : currentIndexMeta.columns) {
            keyLen = etlColumn.isKey ? keyLen + 1 : keyLen;
        }

        SparkRDDAggregator[] sparkRDDAggregators = new SparkRDDAggregator[currentIndexMeta.columns.size() - keyLen];

        for (int i = 0 ; i < currentIndexMeta.columns.size(); i++) {
            if (!currentIndexMeta.columns.get(i).isKey && !isDuplicateTable) {
                sparkRDDAggregators[i - keyLen] = SparkRDDAggregator.buildAggregator(currentIndexMeta.columns.get(i));
            }
        }

        PairFunction<Row, List<Object>, Object[]> encodePairFunction = isDuplicateTable ?
                // add 1 to include bucketId
                new EncodeDuplicateTableFunction(keyLen + 1, currentIndexMeta.columns.size() - keyLen)
                : new EncodeAggregateTableFunction(sparkRDDAggregators, keyLen + 1);

        // 2 convert dataframe to rdd and  encode key and value
        // TODO(wb) use rdd to avoid bitamp/hll serialize when calculate rollup
        JavaPairRDD<List<Object>, Object[]> currentRollupRDD = dataframe.toJavaRDD().mapToPair(encodePairFunction);

        // 3 do aggregate
        // TODO(wb) set the reduce concurrency by statistic instead of hard code 200
        int aggregateConcurrency = 200;
        JavaPairRDD<List<Object>, Object[]> reduceResultRDD = isDuplicateTable ? currentRollupRDD
                : currentRollupRDD.reduceByKey(new AggregateReduceFunction(sparkRDDAggregators), aggregateConcurrency);

        // 4 repartition and finalize value column
        JavaRDD<Row> finalRDD = reduceResultRDD
                .repartitionAndSortWithinPartitions(new BucketPartitioner(bucketKeyMap), new BucketComparator())
                .map(record -> {
                    List<Object> keys = record._1;
                    Object[] values = record._2;
                    int size = keys.size() + values.length;
                    Object[] result = new Object[size];

                    for (int i = 0; i < keys.size(); i++) {
                        result[i] = keys.get(i);
                    }

                    for (int i = keys.size(); i < size; i++) {
                        int valueIdx = i - keys.size();
                        result[i] = isDuplicateTable ? values[valueIdx] : sparkRDDAggregators[valueIdx].finalize(values[valueIdx]);
                    }

                    return RowFactory.create(result);
                });

        // 4 convert to dataframe
        StructType tableSchemaWithBucketId = DppUtils.createDstTableSchema(currentIndexMeta.columns, true, true);
        dataframe = spark.createDataFrame(finalRDD, tableSchemaWithBucketId);
        return dataframe;

    }

    // write data to parquet file by using writing the parquet scheme of spark.
    private void writePartitionedAndSortedDataframeToParquet(Dataset<Row> dataframe,
                                                             String pathPattern,
                                                             long tableId,
                                                             EtlJobConfig.EtlIndex indexMeta) throws UserException {
        StructType outputSchema = dataframe.schema();
        StructType dstSchema = DataTypes.createStructType(
                Arrays.asList(outputSchema.fields()).stream()
                        .filter(field -> !field.name().equalsIgnoreCase(DppUtils.BUCKET_ID))
                        .collect(Collectors.toList()));
        ExpressionEncoder encoder = RowEncoder.apply(dstSchema);
        dataframe.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                // write the data to dst file
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(etlJobConfig.outputPath), conf);
                String lastBucketKey = null;
                ParquetWriter<InternalRow> parquetWriter = null;
                TaskContext taskContext = TaskContext.get();
                long taskAttemptId = taskContext.taskAttemptId();
                String dstPath = "";
                String tmpPath = "";

                while (t.hasNext()) {
                    Row row = t.next();
                    if (row.length() <= 1) {
                        LOG.warn("invalid row:" + row);
                        continue;
                    }


                    String curBucketKey = row.getString(0);
                    List<Object> columnObjects = new ArrayList<>();
                    for (int i = 1; i < row.length(); ++i) {
                        Object columnValue = row.get(i);
                        columnObjects.add(columnValue);
                    }
                    Row rowWithoutBucketKey = RowFactory.create(columnObjects.toArray());
                    // if the bucket key is new, it will belong to a new tablet
                    if (lastBucketKey == null || !curBucketKey.equals(lastBucketKey)) {
                        if (parquetWriter != null) {
                            parquetWriter.close();
                            // rename tmpPath to path
                            try {
                                fs.rename(new Path(tmpPath), new Path(dstPath));
                            } catch (IOException ioe) {
                                LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath + " failed. exception:" + ioe);
                                throw ioe;
                            }
                        }
                        // flush current writer and create a new writer
                        String[] bucketKey = curBucketKey.split("_");
                        if (bucketKey.length != 2) {
                            LOG.warn("invalid bucket key:" + curBucketKey);
                            continue;
                        }
                        int partitionId = Integer.parseInt(bucketKey[0]);
                        int bucketId = Integer.parseInt(bucketKey[1]);
                        dstPath = String.format(pathPattern, tableId, partitionId, indexMeta.indexId,
                                bucketId, indexMeta.schemaHash);
                        tmpPath = dstPath + "." + taskAttemptId;
                        conf.setBoolean("spark.sql.parquet.writeLegacyFormat", false);
                        conf.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false);
                        conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true);
                        conf.setBoolean("spark.sql.parquet.binaryAsString", false);
                        conf.set("spark.sql.parquet.outputTimestampType", "INT96");
                        ParquetWriteSupport.setSchema(dstSchema, conf);
                        ParquetWriteSupport parquetWriteSupport = new ParquetWriteSupport();
                        parquetWriter = new ParquetWriter<InternalRow>(new Path(tmpPath), parquetWriteSupport,
                                CompressionCodecName.SNAPPY, 256 * 1024 * 1024, 16 * 1024, 1024 * 1024,
                                true, false,
                                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
                        if (parquetWriter != null) {
                            LOG.info("[HdfsOperate]>> initialize writer succeed! path:" + tmpPath);
                        }
                        lastBucketKey = curBucketKey;
                    }
                    InternalRow internalRow = encoder.toRow(rowWithoutBucketKey);
                    parquetWriter.write(internalRow);
                }
                if (parquetWriter != null) {
                    parquetWriter.close();
                    try {
                        fs.rename(new Path(tmpPath), new Path(dstPath));
                    } catch (IOException ioe) {
                        LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath + " failed. exception:" + ioe);
                        throw ioe;
                    }
                }
            }
        });
    }

    // TODO(wb) one shuffle to calculate the rollup in the same level
    private void processRollupTree(RollupTreeNode rootNode,
                                   Dataset<Row> rootDataframe,
                                   long tableId, EtlJobConfig.EtlTable tableMeta,
                                   EtlJobConfig.EtlIndex baseIndex) throws UserException {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Long, Dataset<Row>> parentDataframeMap = new HashMap<>();
        parentDataframeMap.put(baseIndex.indexId, rootDataframe);
        Map<Long, Dataset<Row>> childrenDataframeMap = new HashMap<>();
        String pathPattern = etlJobConfig.outputPath + "/" + etlJobConfig.outputFilePattern;
        while (!nodeQueue.isEmpty()) {
            RollupTreeNode curNode = nodeQueue.poll();
            LOG.info("start to process index:" + curNode.indexId);
            if (curNode.children != null) {
                for (RollupTreeNode child : curNode.children) {
                    nodeQueue.offer(child);
                }
            }
            Dataset<Row> curDataFrame = null;
            // column select for rollup
            if (curNode.level != currentLevel) {
                for (Dataset<Row> dataframe : parentDataframeMap.values()) {
                    dataframe.unpersist();
                }
                currentLevel = curNode.level;
                parentDataframeMap.clear();
                parentDataframeMap = childrenDataframeMap;
                childrenDataframeMap = new HashMap<>();
            }

            long parentIndexId = baseIndex.indexId;
            if (curNode.parent != null) {
                parentIndexId = curNode.parent.indexId;
            }

            Dataset<Row> parentDataframe = parentDataframeMap.get(parentIndexId);
            List<Column> columns = new ArrayList<>();
            List<Column> keyColumns = new ArrayList<>();
            Column bucketIdColumn = new Column(DppUtils.BUCKET_ID);
            keyColumns.add(bucketIdColumn);
            columns.add(bucketIdColumn);
            for (String keyName : curNode.keyColumnNames) {
                columns.add(new Column(keyName));
                keyColumns.add(new Column(keyName));
            }
            for (String valueName : curNode.valueColumnNames) {
                columns.add(new Column(valueName));
            }
            Seq<Column> columnSeq = JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq();
            curDataFrame = parentDataframe.select(columnSeq);
            // aggregate and repartition
            curDataFrame = processRDDAggAndRepartition(curDataFrame, curNode.indexMeta);

            childrenDataframeMap.put(curNode.indexId, curDataFrame);

            if (curNode.children != null && curNode.children.size() > 1) {
                // if the children number larger than 1, persist the dataframe for performance
                curDataFrame.persist();
            }
            writePartitionedAndSortedDataframeToParquet(curDataFrame, pathPattern, tableId, curNode.indexMeta);
        }
    }

    // repartition dataframe by partitionid_bucketid
    // so data in the same bucket will be consecutive.
    private Dataset<Row> repartitionDataframeByBucketId(SparkSession spark, Dataset<Row> dataframe,
                                                        EtlJobConfig.EtlPartitionInfo partitionInfo,
                                                        List<Integer> partitionKeyIndex,
                                                        List<Class> partitionKeySchema,
                                                        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys,
                                                        List<String> keyColumnNames,
                                                        List<String> valueColumnNames,
                                                        StructType dstTableSchema,
                                                        EtlJobConfig.EtlIndex baseIndex,
                                                        List<Long> validPartitionIds) throws UserException {
        List<String> distributeColumns = partitionInfo.distributionColumnRefs;
        Partitioner partitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);
        Set<Integer> validPartitionIndex = new HashSet<>();
        if (validPartitionIds == null) {
            for (int i = 0; i < partitionInfo.partitions.size(); ++i) {
                validPartitionIndex.add(i);
            }
        } else {
            for (int i = 0; i < partitionInfo.partitions.size(); ++i) {
                if (validPartitionIds.contains(partitionInfo.partitions.get(i).partitionId)) {
                    validPartitionIndex.add(i);
                }
            }
        }
        // use PairFlatMapFunction instead of PairMapFunction because the there will be
        // 0 or 1 output row for 1 input row
        JavaPairRDD<String, DppColumns> pairRDD = dataframe.javaRDD().flatMapToPair(
                new PairFlatMapFunction<Row, String, DppColumns>() {
                    @Override
                    public Iterator<Tuple2<String, DppColumns>> call(Row row) {
                        List<Object> columns = new ArrayList<>();
                        List<Object> keyColumns = new ArrayList<>();
                        for (String columnName : keyColumnNames) {
                            Object columnObject = row.get(row.fieldIndex(columnName));
                            columns.add(columnObject);
                            keyColumns.add(columnObject);
                        }

                        for (String columnName : valueColumnNames) {
                            columns.add(row.get(row.fieldIndex(columnName)));
                        }
                        DppColumns dppColumns = new DppColumns(columns);
                        DppColumns key = new DppColumns(keyColumns);
                        List<Tuple2<String, DppColumns>> result = new ArrayList<>();
                        int pid = partitioner.getPartition(key);
                        if (!validPartitionIndex.contains(pid)) {
                            LOG.warn("invalid partition for row:" + row + ", pid:" + pid);
                            abnormalRowAcc.add(1);
                            LOG.info("abnormalRowAcc:" + abnormalRowAcc);
                            if (abnormalRowAcc.value() < 5) {
                                LOG.info("add row to invalidRows:" + row.toString());
                                invalidRows.add(row.toString());
                                LOG.info("invalid rows contents:" + invalidRows.value());
                            }
                        } else {
                            long hashValue = DppUtils.getHashValue(row, distributeColumns, dstTableSchema);
                            int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
                            long partitionId = partitionInfo.partitions.get(pid).partitionId;
                            // bucketKey is partitionId_bucketId
                            String bucketKey = partitionId + "_" + bucketId;
                            Tuple2<String, DppColumns> newTuple = new Tuple2<String, DppColumns>(bucketKey, dppColumns);
                            result.add(newTuple);
                        }
                        return result.iterator();
                    }
                });
        // TODO(wb): using rdd instead of dataframe from here
        JavaRDD<Row> resultRdd = pairRDD.map(record -> {
                    String bucketKey = record._1;
                    List<Object> row = new ArrayList<>();
                    // bucketKey as the first key
                    row.add(bucketKey);
                    row.addAll(record._2.columns);
                    return RowFactory.create(row.toArray());
                }
        );

        StructType tableSchemaWithBucketId = DppUtils.createDstTableSchema(baseIndex.columns, true, false);
        dataframe = spark.createDataFrame(resultRdd, tableSchemaWithBucketId);
        // use bucket number as the parallel number
        int reduceNum = 0;
        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            for (int i = 0; i < partition.bucketNum; i++) {
                bucketKeyMap.put(partition.partitionId + "_" + i, reduceNum);
                reduceNum++;
            }
        }

        // print to system.out for easy to find log info
        System.out.println("print bucket key map:" + bucketKeyMap.toString());

        return dataframe;
    }

    // do the etl process
    private Dataset<Row> convertSrcDataframeToDstDataframe(EtlJobConfig.EtlIndex baseIndex,
                                                           Dataset<Row> srcDataframe,
                                                           StructType dstTableSchema,
                                                           EtlJobConfig.EtlFileGroup fileGroup) throws UserException {
        Dataset<Row> dataframe = srcDataframe;
        StructType srcSchema = dataframe.schema();
        Set<String> srcColumnNames = new HashSet<>();
        for (StructField field : srcSchema.fields()) {
            srcColumnNames.add(field.name());
        }
        Map<String, EtlJobConfig.EtlColumnMapping> columnMappings = fileGroup.columnMappings;
        // 1. process simple columns
        Set<String> mappingColumns = null;
        if (columnMappings != null) {
            mappingColumns = columnMappings.keySet();
        }
        List<String> dstColumnNames = new ArrayList<>();
        for (StructField dstField : dstTableSchema.fields()) {
            dstColumnNames.add(dstField.name());
            EtlJobConfig.EtlColumn column = baseIndex.getColumn(dstField.name());
            if (!srcColumnNames.contains(dstField.name())) {
                if (mappingColumns != null && mappingColumns.contains(dstField.name())) {
                    // mapping columns will be processed in next step
                    continue;
                }
                if (column.defaultValue != null) {
                    if (column.defaultValue.equals(NULL_FLAG)) {
                        dataframe = dataframe.withColumn(dstField.name(), functions.lit(null));
                    } else {
                        dataframe = dataframe.withColumn(dstField.name(), functions.lit(column.defaultValue));
                    }
                } else if (column.isAllowNull) {
                    dataframe = dataframe.withColumn(dstField.name(), functions.lit(null));
                } else {
                    throw new UserException("Reason: no data for column:" + dstField.name());
                }
            }
            if (column.columnType.equalsIgnoreCase("DATE")) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast("date"));
            } else if (column.columnType.equalsIgnoreCase("BOOLEAN")) {
                dataframe = dataframe.withColumn(dstField.name(),
                        functions.when(dataframe.col(dstField.name()).equalTo("true"), "1")
                                .otherwise("0"));
            } else if (!column.columnType.equalsIgnoreCase(BITMAP_TYPE) && !dstField.dataType().equals(DataTypes.StringType)) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(dstField.dataType()));
            }
            if (fileGroup.isNegative && !column.isKey) {
                // negative load
                // value will be convert te -1 * value
                dataframe = dataframe.withColumn(dstField.name(), functions.expr("-1 *" + dstField.name()));
            }
        }
        // 2. process the mapping columns
        for (String mappingColumn : mappingColumns) {
            String mappingDescription = columnMappings.get(mappingColumn).toDescription();
            if (mappingDescription.toLowerCase().contains("hll_hash")) {
                continue;
            }
            // here should cast data type to dst column type
            dataframe = dataframe.withColumn(mappingColumn,
                    functions.expr(mappingDescription).cast(dstTableSchema.apply(mappingColumn).dataType()));
        }
        // projection and reorder the columns
        dataframe.createOrReplaceTempView("src_table");
        StringBuilder selectSqlBuilder = new StringBuilder();
        selectSqlBuilder.append("select ");
        for (String name : dstColumnNames) {
            selectSqlBuilder.append(name + ",");
        }
        selectSqlBuilder.deleteCharAt(selectSqlBuilder.length() - 1);
        selectSqlBuilder.append(" from src_table");
        String selectSql = selectSqlBuilder.toString();
        dataframe = spark.sql(selectSql);
        return dataframe;
    }

    private Dataset<Row> loadDataFromPath(SparkSession spark,
                                          EtlJobConfig.EtlFileGroup fileGroup,
                                          String fileUrl,
                                          EtlJobConfig.EtlIndex baseIndex,
                                          List<EtlJobConfig.EtlColumn> columns) throws UserException {
        List<String> columnValueFromPath = DppUtils.parseColumnsFromPath(fileUrl, fileGroup.columnsFromPath);
        List<String> dataSrcColumns = fileGroup.fileFieldNames;
        if (dataSrcColumns == null) {
            // if there is no source columns info
            // use base index columns as source columns
            dataSrcColumns = new ArrayList<>();
            for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
                dataSrcColumns.add(column.columnName);
            }
        }
        List<String> dstTableNames = new ArrayList<>();
        for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
            dstTableNames.add(column.columnName);
        }
        List<String> srcColumnsWithColumnsFromPath = new ArrayList<>();
        srcColumnsWithColumnsFromPath.addAll(dataSrcColumns);
        if (fileGroup.columnsFromPath != null) {
            srcColumnsWithColumnsFromPath.addAll(fileGroup.columnsFromPath);
        }
        StructType srcSchema = createScrSchema(srcColumnsWithColumnsFromPath);
        JavaRDD<String> sourceDataRdd = spark.read().textFile(fileUrl).toJavaRDD();
        int columnSize = dataSrcColumns.size();
        List<ColumnParser> parsers = new ArrayList<>();
        for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
            parsers.add(ColumnParser.create(column));
        }
        // now we first support csv file
        // TODO: support parquet file and orc file
        JavaRDD<Row> rowRDD = sourceDataRdd.flatMap(
                record -> {
                    scannedRowsAcc.add(1);
                    String[] attributes = record.split(fileGroup.columnSeparator);
                    List<Row> result = new ArrayList<>();
                    boolean validRow = true;
                    if (attributes.length != columnSize) {
                        LOG.warn("invalid src schema, data columns:"
                                + attributes.length + ", file group columns:"
                                + columnSize + ", row:" + record);
                        validRow = false;
                    } else {
                        for (int i = 0; i < attributes.length; ++i) {
                            if (attributes[i].equals(NULL_FLAG)) {
                                if (baseIndex.columns.get(i).isAllowNull) {
                                    attributes[i] = null;
                                } else {
                                    LOG.warn("colunm:" + i + " can not be null. row:" + record);
                                    validRow = false;
                                    break;
                                }
                            }
                            boolean isStrictMode = (boolean) etlJobConfig.properties.strictMode;
                            if (isStrictMode) {
                                StructField field = srcSchema.apply(i);
                                if (dstTableNames.contains(field.name())) {
                                    String type = columns.get(i).columnType;
                                    if (type.equalsIgnoreCase("CHAR")
                                            || type.equalsIgnoreCase("VARCHAR")) {
                                        continue;
                                    }
                                    ColumnParser parser = parsers.get(i);
                                    boolean valid = parser.parse(attributes[i]);
                                    if (!valid) {
                                        validRow = false;
                                        LOG.warn("invalid row:" + record
                                                + ", attribute " + i + ": " + attributes[i] + " parsed failed");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (validRow) {
                        Row row = null;
                        if (fileGroup.columnsFromPath == null) {
                            row = RowFactory.create(attributes);
                        } else {
                            // process columns from path
                            // append columns from path to the tail
                            List<String> columnAttributes = new ArrayList<>();
                            columnAttributes.addAll(Arrays.asList(attributes));
                            columnAttributes.addAll(columnValueFromPath);
                            row = RowFactory.create(columnAttributes.toArray());
                        }
                        result.add(row);
                    } else {
                        abnormalRowAcc.add(1);
                        // at most add 5 rows to invalidRows
                        if (abnormalRowAcc.value() <= 5) {
                            invalidRows.add(record);
                        }
                    }
                    return result.iterator();
                }
        );

        Dataset<Row> dataframe = spark.createDataFrame(rowRDD, srcSchema);
        return dataframe;
    }

    private StructType createScrSchema(List<String> srcColumns) {
        List<StructField> fields = new ArrayList<>();
        for (String srcColumn : srcColumns) {
            // user StringType to load source data
            StructField field = DataTypes.createStructField(srcColumn, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType srcSchema = DataTypes.createStructType(fields);
        return srcSchema;
    }

    // partition keys will be parsed into double from json
    // so need to convert it to partition columns' type
    private Object convertPartitionKey(Object srcValue, Class dstClass) throws UserException {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                return ((Double) srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double) srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                return ((Double) srcValue).longValue();
            } else if (dstClass.equals(BigInteger.class)) {
                return new BigInteger(((Double) srcValue).toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double)srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double)srcValue;
                return convertToJavaDatetime((long)srcValueDouble);
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else {
            LOG.warn("unsupport partition key:" + srcValue);
            throw new UserException("unsupport partition key:" + srcValue);
        }
    }

    private java.sql.Timestamp convertToJavaDatetime(long src) {
        String dateTimeStr = Long.valueOf(src).toString();
        if (dateTimeStr.length() != 14) {
            throw new RuntimeException("invalid input date format for SparkDpp");
        }

        String year = dateTimeStr.substring(0, 4);
        String month = dateTimeStr.substring(4, 6);
        String day = dateTimeStr.substring(6, 8);
        String hour = dateTimeStr.substring(8, 10);
        String min = dateTimeStr.substring(10, 12);
        String sec = dateTimeStr.substring(12, 14);

        return java.sql.Timestamp.valueOf(String.format("%s-%s-%s %s:%s:%s", year, month, day, hour, min, sec));
    }

    private java.sql.Date convertToJavaDate(int originDate) {
        int day = originDate & 0x1f;
        originDate >>= 5;
        int month = originDate & 0x0f;
        originDate >>= 4;
        int year = originDate;
        return java.sql.Date.valueOf(String.format("%04d-%02d-%02d", year, month, day));
    }

    private List<DorisRangePartitioner.PartitionRangeKey> createPartitionRangeKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws UserException {
        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            DorisRangePartitioner.PartitionRangeKey partitionRangeKey = new DorisRangePartitioner.PartitionRangeKey();
            List<Object> startKeyColumns = new ArrayList<>();
            for (int i = 0; i < partition.startKeys.size(); i++) {
                Object value = partition.startKeys.get(i);
                startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
            }
            partitionRangeKey.startKeys = new DppColumns(startKeyColumns);
            if (!partition.isMaxPartition) {
                partitionRangeKey.isMaxPartition = false;
                List<Object> endKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.endKeys.size(); i++) {
                    Object value = partition.endKeys.get(i);
                    endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.endKeys = new DppColumns(endKeyColumns);
            } else {
                partitionRangeKey.isMaxPartition = true;
            }
            partitionRangeKeys.add(partitionRangeKey);
        }
        return partitionRangeKeys;
    }

    private Dataset<Row> loadDataFromFilePaths(SparkSession spark,
                                               EtlJobConfig.EtlIndex baseIndex,
                                               List<String> filePaths,
                                               EtlJobConfig.EtlFileGroup fileGroup,
                                               StructType dstTableSchema)
            throws UserException, IOException, URISyntaxException {
        Dataset<Row> fileGroupDataframe = null;
        for (String filePath : filePaths) {
            fileNumberAcc.add(1);
            try {
                Configuration conf = new Configuration();
                URI uri = new URI(filePath);
                FileSystem fs = FileSystem.get(uri, conf);
                FileStatus fileStatus = fs.getFileStatus(new Path(filePath));
                fileSizeAcc.add(fileStatus.getLen());
            } catch (Exception e) {
                LOG.warn("parse path failed:" + filePath);
                throw e;
            }
            if (fileGroup.columnSeparator == null) {
                LOG.warn("invalid null column separator!");
                throw new UserException("Reason: invalid null column separator!");
            }
            Dataset<Row> dataframe = null;

            dataframe = loadDataFromPath(spark, fileGroup, filePath, baseIndex, baseIndex.columns);
            dataframe = convertSrcDataframeToDstDataframe(baseIndex, dataframe, dstTableSchema, fileGroup);
            if (fileGroupDataframe == null) {
                fileGroupDataframe = dataframe;
            } else {
                fileGroupDataframe.union(dataframe);
            }
        }
        return fileGroupDataframe;
    }

    private Dataset<Row> loadDataFromHiveTable(SparkSession spark,
                                               String hiveDbTableName,
                                               EtlJobConfig.EtlIndex baseIndex,
                                               EtlJobConfig.EtlFileGroup fileGroup,
                                               StructType dstTableSchema) throws UserException {
        // select base index columns from hive table
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        baseIndex.columns.forEach(column -> {
            sql.append(column.columnName).append(",");
        });
        sql.deleteCharAt(sql.length() - 1).append(" from ").append(hiveDbTableName);
        Dataset<Row> dataframe = spark.sql(sql.toString());
        dataframe = convertSrcDataframeToDstDataframe(baseIndex, dataframe, dstTableSchema, fileGroup);
        return dataframe;
    }

    private DppResult process() throws Exception {
        DppResult dppResult = new DppResult();
        try {
            for (Map.Entry<Long, EtlJobConfig.EtlTable> entry : etlJobConfig.tables.entrySet()) {
                Long tableId = entry.getKey();
                EtlJobConfig.EtlTable etlTable = entry.getValue();

                // get the base index meta
                EtlJobConfig.EtlIndex baseIndex = null;
                for (EtlJobConfig.EtlIndex indexMeta : etlTable.indexes) {
                    if (indexMeta.isBaseIndex) {
                        baseIndex = indexMeta;
                        break;
                    }
                }

                // get key column names and value column names seperately
                List<String> keyColumnNames = new ArrayList<>();
                List<String> valueColumnNames = new ArrayList<>();
                for (EtlJobConfig.EtlColumn etlColumn : baseIndex.columns) {
                    if (etlColumn.isKey) {
                        keyColumnNames.add(etlColumn.columnName);
                    } else {
                        valueColumnNames.add(etlColumn.columnName);
                    }
                }

                EtlJobConfig.EtlPartitionInfo partitionInfo = etlTable.partitionInfo;
                List<Integer> partitionKeyIndex = new ArrayList<Integer>();
                List<Class> partitionKeySchema = new ArrayList<>();
                for (String key : partitionInfo.partitionColumnRefs) {
                    for (int i = 0; i < baseIndex.columns.size(); ++i) {
                        EtlJobConfig.EtlColumn column = baseIndex.columns.get(i);
                        if (column.columnName.equals(key)) {
                            partitionKeyIndex.add(i);
                            partitionKeySchema.add(DppUtils.getClassFromColumn(column));
                            break;
                        }
                    }
                }
                List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = createPartitionRangeKeys(partitionInfo, partitionKeySchema);
                StructType dstTableSchema = DppUtils.createDstTableSchema(baseIndex.columns, false, false);
                RollupTreeBuilder rollupTreeParser = new MinimumCoverageRollupTreeBuilder();
                RollupTreeNode rootNode = rollupTreeParser.build(etlTable);
                LOG.info("Start to process rollup tree:" + rootNode);

                Dataset<Row> tableDataframe = null;
                for (EtlJobConfig.EtlFileGroup fileGroup : etlTable.fileGroups) {
                    List<String> filePaths = fileGroup.filePaths;
                    Dataset<Row> fileGroupDataframe = null;
                    EtlJobConfig.SourceType sourceType = fileGroup.sourceType;
                    if (sourceType == EtlJobConfig.SourceType.FILE) {
                        fileGroupDataframe = loadDataFromFilePaths(spark, baseIndex, filePaths, fileGroup, dstTableSchema);
                    } else if (sourceType == EtlJobConfig.SourceType.HIVE) {
                        fileGroupDataframe = loadDataFromHiveTable(spark, fileGroup.dppHiveDbTableName, baseIndex, fileGroup, dstTableSchema);
                    } else {
                        throw new RuntimeException("Unknown source type: " + sourceType.name());
                    }
                    if (fileGroupDataframe == null) {
                        LOG.info("no data for file file group:" + fileGroup);
                        continue;
                    }
                    if (!Strings.isNullOrEmpty(fileGroup.where)) {
                        long originalSize = fileGroupDataframe.count();
                        fileGroupDataframe = fileGroupDataframe.filter(fileGroup.where);
                        long currentSize = fileGroupDataframe.count();
                        unselectedRowAcc.add(currentSize - originalSize);
                    }

                    fileGroupDataframe = repartitionDataframeByBucketId(spark, fileGroupDataframe,
                            partitionInfo, partitionKeyIndex,
                            partitionKeySchema, partitionRangeKeys,
                            keyColumnNames, valueColumnNames,
                            dstTableSchema, baseIndex, fileGroup.partitions);
                    if (tableDataframe == null) {
                        tableDataframe = fileGroupDataframe;
                    } else {
                        tableDataframe.union(fileGroupDataframe);
                    }
                }
                processRollupTree(rootNode, tableDataframe, tableId, etlTable, baseIndex);
            }
            spark.stop();
        } catch (Exception exception) {
            LOG.warn("spark dpp failed for exception:" + exception);
            dppResult.isSuccess = false;
            dppResult.failedReason = exception.getMessage();
            dppResult.normalRows = scannedRowsAcc.value() - abnormalRowAcc.value();
            dppResult.scannedRows = scannedRowsAcc.value();
            dppResult.fileNumber = fileNumberAcc.value();
            dppResult.fileSize = fileSizeAcc.value();
            dppResult.abnormalRows = abnormalRowAcc.value();
            dppResult.partialAbnormalRows = invalidRows.value();
            throw exception;
        }
        LOG.info("invalid rows contents:" + invalidRows.value());
        dppResult.isSuccess = true;
        dppResult.failedReason = "";
        dppResult.normalRows = scannedRowsAcc.value() - abnormalRowAcc.value();
        dppResult.scannedRows = scannedRowsAcc.value();
        dppResult.fileNumber = fileNumberAcc.value();
        dppResult.fileSize = fileSizeAcc.value();
        dppResult.abnormalRows = abnormalRowAcc.value();
        dppResult.partialAbnormalRows = invalidRows.value();
        return dppResult;
    }

    public void doDpp() throws Exception {
        // write dpp result to output
        DppResult dppResult = process();
        String outputPath = etlJobConfig.getOutputPath();
        String resultFilePath = outputPath + "/" + DPP_RESULT_FILE;
        Configuration conf = new Configuration();
        URI uri = new URI(outputPath);
        FileSystem fs = FileSystem.get(uri, conf);
        Path filePath = new Path(resultFilePath);
        FSDataOutputStream outputStream = fs.create(filePath);
        Gson gson = new Gson();
        outputStream.write(gson.toJson(dppResult).getBytes());
        outputStream.write('\n');
        outputStream.close();
    }
}