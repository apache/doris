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

import scala.Tuple2;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import com.google.common.base.Strings;
import com.google.gson.Gson;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.util.SerializableConfiguration;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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
    // save the hadoop configuration from spark session.
    // because hadoop configuration is not serializable,
    // we need to wrap it so that we can use it in executor.
    private SerializableConfiguration serializableHadoopConf;
    private DppResult dppResult = new DppResult();


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
        this.serializableHadoopConf = new SerializableConfiguration(spark.sparkContext().hadoopConfiguration());
    }

    private JavaPairRDD<List<Object>, Object[]> processRDDAggregate(JavaPairRDD<List<Object>, Object[]> currentPairRDD, RollupTreeNode curNode,
                                                      SparkRDDAggregator[] sparkRDDAggregators) throws SparkDppException {
        final boolean isDuplicateTable = !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "AGGREGATE")
                && !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "UNIQUE");

        // Aggregate/UNIQUE table
        if (!isDuplicateTable) {
            // TODO(wb) set the reduce concurrency by statistic instead of hard code 200
            int aggregateConcurrency = 200;

            int idx = 0;
            for (int i = 0 ; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    sparkRDDAggregators[idx] = SparkRDDAggregator.buildAggregator(curNode.indexMeta.columns.get(i));
                    idx++;
                }
            }

            if (curNode.indexMeta.isBaseIndex) {
                JavaPairRDD<List<Object>, Object[]> result = currentPairRDD.mapToPair(new EncodeBaseAggregateTableFunction(sparkRDDAggregators))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators), aggregateConcurrency);
                return result;
            } else {
                JavaPairRDD<List<Object>, Object[]> result = currentPairRDD
                        .mapToPair(new EncodeRollupAggregateTableFunction(
                                getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                                        curNode.parent.keyColumnNames, curNode.parent.valueColumnNames)))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators), aggregateConcurrency);
                return result;
            }
        // Duplicate Table
        } else {
            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    // duplicate table doesn't need aggregator
                    // init a aggregator here just for keeping interface compatibility when writing data to HDFS
                    sparkRDDAggregators[idx] = new DefaultSparkRDDAggregator();
                    idx++;
                }
            }
            if (curNode.indexMeta.isBaseIndex) {
                return currentPairRDD;
            } else {
                return currentPairRDD.mapToPair(new EncodeRollupAggregateTableFunction(
                        getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                        curNode.parent.keyColumnNames, curNode.parent.valueColumnNames)));
            }
        }
    }

    // write data to parquet file by using writing the parquet scheme of spark.
    private void writeRepartitionAndSortedRDDToParquet(JavaPairRDD<List<Object>, Object[]> resultRDD,
                                                            String pathPattern,
                                                             long tableId,
                                                             EtlJobConfig.EtlIndex indexMeta,
                                                             SparkRDDAggregator[] sparkRDDAggregators) throws SparkDppException {
        StructType dstSchema = DppUtils.createDstTableSchema(indexMeta.columns, false, true);
        ExpressionEncoder encoder = RowEncoder.apply(dstSchema);

        resultRDD.repartitionAndSortWithinPartitions(new BucketPartitioner(bucketKeyMap), new BucketComparator())
        .foreachPartition(new VoidFunction<Iterator<Tuple2<List<Object>,Object[]>>>() {
            @Override
            public void call(Iterator<Tuple2<List<Object>, Object[]>> t) throws Exception {
                // write the data to dst file
                Configuration conf = new Configuration(serializableHadoopConf.value());
                FileSystem fs = FileSystem.get(URI.create(etlJobConfig.outputPath), conf);
                String lastBucketKey = null;
                ParquetWriter<InternalRow> parquetWriter = null;
                TaskContext taskContext = TaskContext.get();
                long taskAttemptId = taskContext.taskAttemptId();
                String dstPath = "";
                String tmpPath = "";

                while (t.hasNext()) {
                    Tuple2<List<Object>, Object[]> pair = t.next();
                    List<Object> keyColumns = pair._1();
                    Object[] valueColumns = pair._2();
                    if ((keyColumns.size() + valueColumns.length) <= 1) {
                        LOG.warn("invalid row:" + pair);
                        continue;
                    }


                    String curBucketKey = keyColumns.get(0).toString();
                    List<Object> columnObjects = new ArrayList<>();
                    for (int i = 1; i < keyColumns.size(); ++i) {
                        columnObjects.add(keyColumns.get(i));
                    }
                    for (int i = 0; i < valueColumns.length; ++i) {
                        columnObjects.add(sparkRDDAggregators[i].finalize(valueColumns[i]));
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
        });}

    // TODO(wb) one shuffle to calculate the rollup in the same level
    private void processRollupTree(RollupTreeNode rootNode,
                                   JavaPairRDD<List<Object>, Object[]> rootRDD,
                                   long tableId, EtlJobConfig.EtlIndex baseIndex) throws SparkDppException {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Long, JavaPairRDD<List<Object>, Object[]>> parentRDDMap = new HashMap<>();
        parentRDDMap.put(baseIndex.indexId, rootRDD);
        Map<Long, JavaPairRDD<List<Object>, Object[]>> childrenRDDMap = new HashMap<>();
        String pathPattern = etlJobConfig.outputPath + "/" + etlJobConfig.outputFilePattern;
        while (!nodeQueue.isEmpty()) {
            RollupTreeNode curNode = nodeQueue.poll();
            LOG.info("start to process index:" + curNode.indexId);
            if (curNode.children != null) {
                for (RollupTreeNode child : curNode.children) {
                    nodeQueue.offer(child);
                }
            }
            JavaPairRDD<List<Object>, Object[]> curRDD = null;
            // column select for rollup
            if (curNode.level != currentLevel) {
                for (JavaPairRDD<List<Object>, Object[]> rdd : parentRDDMap.values()) {
                    rdd.unpersist();
                }
                currentLevel = curNode.level;
                parentRDDMap.clear();
                parentRDDMap = childrenRDDMap;
                childrenRDDMap = new HashMap<>();
            }

            long parentIndexId = baseIndex.indexId;
            if (curNode.parent != null) {
                parentIndexId = curNode.parent.indexId;
            }

            JavaPairRDD<List<Object>, Object[]> parentRDD = parentRDDMap.get(parentIndexId);

            // aggregate
            SparkRDDAggregator[] sparkRDDAggregators = new SparkRDDAggregator[curNode.valueColumnNames.size()];
            curRDD = processRDDAggregate(parentRDD, curNode, sparkRDDAggregators);

            childrenRDDMap.put(curNode.indexId, curRDD);

            if (curNode.children != null && curNode.children.size() > 1) {
                // if the children number larger than 1, persist the dataframe for performance
                curRDD.persist(StorageLevel.MEMORY_AND_DISK());
            }
            // repartition and write to hdfs
            writeRepartitionAndSortedRDDToParquet(curRDD, pathPattern, tableId, curNode.indexMeta, sparkRDDAggregators);
        }
    }

    // get column index map from parent rollup to child rollup
    // not consider bucketId here
    private Pair<Integer[], Integer[]> getColumnIndexInParentRollup(List<String> childRollupKeyColumns, List<String> childRollupValueColumns,
                                                                            List<String> parentRollupKeyColumns, List<String> parentRollupValueColumns) throws SparkDppException {
        List<Integer> keyMap = new ArrayList<>();
        List<Integer> valueMap = new ArrayList<>();
        // find column index in parent rollup schema
        for (int i = 0; i < childRollupKeyColumns.size(); i++) {
            for (int j = 0; j < parentRollupKeyColumns.size(); j++) {
                if (StringUtils.equalsIgnoreCase(childRollupKeyColumns.get(i), parentRollupKeyColumns.get(j))) {
                    keyMap.add(j);
                    break;
                }
            }
        }

        for (int i = 0; i < childRollupValueColumns.size(); i++) {
            for (int j = 0; j < parentRollupValueColumns.size(); j++) {
                if (StringUtils.equalsIgnoreCase(childRollupValueColumns.get(i), parentRollupValueColumns.get(j))) {
                    valueMap.add(j);
                    break;
                }
            }
        }

        if (keyMap.size() != childRollupKeyColumns.size() || valueMap.size() != childRollupValueColumns.size()) {
            throw new SparkDppException(String.format("column map index from child to parent has error, key size src: %s, dst: %s; value size src: %s, dst: %s",
                    childRollupKeyColumns.size(), keyMap.size(), childRollupValueColumns.size(), valueMap.size()));
        }

        return Pair.of(keyMap.toArray(new Integer[keyMap.size()]), valueMap.toArray(new Integer[valueMap.size()]));
    }

    // repartition dataframe by partitionid_bucketid
    // so data in the same bucket will be consecutive.
    private JavaPairRDD<List<Object>, Object[]> fillTupleWithPartitionColumn(SparkSession spark, Dataset<Row> dataframe,
                                                        EtlJobConfig.EtlPartitionInfo partitionInfo,
                                                        List<Integer> partitionKeyIndex,
                                                        List<Class> partitionKeySchema,
                                                        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys,
                                                        List<String> keyColumnNames,
                                                        List<String> valueColumnNames,
                                                        StructType dstTableSchema,
                                                        EtlJobConfig.EtlIndex baseIndex,
                                                        List<Long> validPartitionIds) throws SparkDppException {
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
        JavaPairRDD<List<Object>, Object[]> resultPairRDD = dataframe.toJavaRDD().flatMapToPair(new PairFlatMapFunction<Row, List<Object>, Object[]>() {
            @Override
            public Iterator<Tuple2<List<Object>, Object[]>> call(Row row) throws Exception {
                List<Object> keyColumns = new ArrayList<>();
                Object[] valueColumns = new Object[valueColumnNames.size()];
                for (String columnName : keyColumnNames) {
                    Object columnObject = row.get(row.fieldIndex(columnName));
                    keyColumns.add(columnObject);
                }

                for (int i = 0; i < valueColumnNames.size(); i++) {
                    valueColumns[i] = row.get(row.fieldIndex(valueColumnNames.get(i)));
                }

                DppColumns key = new DppColumns(keyColumns);
                int pid = partitioner.getPartition(key);
                List<Tuple2<List<Object>, Object[]>> result = new ArrayList<>();
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

                    List<Object> tuple = new ArrayList<>();
                    tuple.add(bucketKey);
                    tuple.addAll(keyColumns);
                    result.add(new Tuple2<>(tuple, valueColumns));
                }
                return result.iterator();
            }
        });

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

        return resultPairRDD;
    }

    // do the etl process
    private Dataset<Row> convertSrcDataframeToDstDataframe(EtlJobConfig.EtlIndex baseIndex,
                                                           Dataset<Row> srcDataframe,
                                                           StructType dstTableSchema,
                                                           EtlJobConfig.EtlFileGroup fileGroup) throws SparkDppException {
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
                    throw new SparkDppException("Reason: no data for column:" + dstField.name());
                }
            }
            if (column.columnType.equalsIgnoreCase("DATE")) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast("date"));
            } else if (column.columnType.equalsIgnoreCase("BOOLEAN")) {
                dataframe = dataframe.withColumn(dstField.name(),
                        functions.when(functions.lower(dataframe.col(dstField.name())).equalTo("true"), "1")
                                .when(dataframe.col(dstField.name()).equalTo("1"), "1")
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
                                          List<EtlJobConfig.EtlColumn> columns) throws SparkDppException {
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
        // for getting schema to check source data
        Map<String, Integer> dstColumnNameToIndex = new HashMap<String, Integer>();
        for (int i = 0; i < baseIndex.columns.size(); i++) {
            dstColumnNameToIndex.put(baseIndex.columns.get(i).columnName, i);
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
        char separator = (char)fileGroup.columnSeparator.getBytes(Charset.forName("UTF-8"))[0];
        // now we first support csv file
        // TODO: support parquet file and orc file
        JavaRDD<Row> rowRDD = sourceDataRdd.flatMap(
                record -> {
                    scannedRowsAcc.add(1);
                    String[] attributes = splitLine(record, separator);
                    List<Row> result = new ArrayList<>();
                    boolean validRow = true;
                    if (attributes.length != columnSize) {
                        LOG.warn("invalid src schema, data columns:"
                                + attributes.length + ", file group columns:"
                                + columnSize + ", row:" + record);
                        validRow = false;
                    } else {
                        for (int i = 0; i < attributes.length; ++i) {
                            StructField field = srcSchema.apply(i);
                            String srcColumnName = field.name();
                            if (attributes[i].equals(NULL_FLAG) && dstColumnNameToIndex.containsKey(srcColumnName)) {
                                if (baseIndex.columns.get(dstColumnNameToIndex.get(srcColumnName)).isAllowNull) {
                                    attributes[i] = null;
                                } else {
                                    LOG.warn("column name:" + srcColumnName + ", attribute: " + i
                                            + " can not be null. row:" + record);
                                    validRow = false;
                                    break;
                                }
                            }
                            boolean isStrictMode = etlJobConfig.properties.strictMode;
                            if (isStrictMode) {
                                if (dstColumnNameToIndex.containsKey(srcColumnName)) {
                                    int index = dstColumnNameToIndex.get(srcColumnName);
                                    String type = columns.get(index).columnType;
                                    if (type.equalsIgnoreCase("CHAR")
                                            || type.equalsIgnoreCase("VARCHAR")) {
                                        continue;
                                    }
                                    ColumnParser parser = parsers.get(index);
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

    // This method is to keep the splitting consistent with broker load / mini load
    private String[] splitLine(String line, char sep) {
        if (line == null || line.equals("")) {
            return new String[0];
        }
        int index = 0;
        int lastIndex = 0;
        // line-begin char and line-end char are considered to be 'delimeter'
        List<String> values = new ArrayList<>();
        for (int i = 0 ; i < line.length(); i++, index++) {
            if (line.charAt(index) == sep) {
                values.add(line.substring(lastIndex, index));
                lastIndex = index + 1;
            }
        }
        values.add(line.substring(lastIndex, index));
        return values.toArray(new String[0]);
    }

    // partition keys will be parsed into double from json
    // so need to convert it to partition columns' type
    private Object convertPartitionKey(Object srcValue, Class dstClass) throws SparkDppException {
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
            throw new SparkDppException("unsupport partition key:" + srcValue);
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
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) throws SparkDppException {
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
            throws SparkDppException, IOException, URISyntaxException {
        Dataset<Row> fileGroupDataframe = null;
        for (String filePath : filePaths) {
            try {
                URI uri = new URI(filePath);
                FileSystem fs = FileSystem.get(uri, serializableHadoopConf.value());
                FileStatus[] fileStatuses = fs.globStatus(new Path(filePath));
                if (fileStatuses == null) {
                    throw new SparkDppException("fs list status failed: " + filePath);
                }
                for (FileStatus fileStatus : fileStatuses) {
                    if (fileStatus.isDirectory()) {
                        continue;
                    }
                    fileNumberAcc.add(1);
                    fileSizeAcc.add(fileStatus.getLen());
                }
            } catch (Exception e) {
                LOG.warn("parse path failed:" + filePath);
                throw e;
            }
            if (fileGroup.columnSeparator == null) {
                LOG.warn("invalid null column separator!");
                throw new SparkDppException("Reason: invalid null column separator!");
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
                                               StructType dstTableSchema) throws SparkDppException {
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

    private void process() throws Exception {
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

                JavaPairRDD<List<Object>, Object[]> tablePairRDD = null;
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

                    JavaPairRDD<List<Object>, Object[]> ret = fillTupleWithPartitionColumn(spark, fileGroupDataframe,
                            partitionInfo, partitionKeyIndex,
                            partitionKeySchema, partitionRangeKeys,
                            keyColumnNames, valueColumnNames,
                            dstTableSchema, baseIndex, fileGroup.partitions);
                    if (tablePairRDD == null) {
                        tablePairRDD = ret;
                    } else {
                        tablePairRDD.union(ret);
                    }
                }
                processRollupTree(rootNode, tablePairRDD, tableId, baseIndex);
            }
            LOG.info("invalid rows contents:" + invalidRows.value());
            dppResult.isSuccess = true;
            dppResult.failedReason = "";
        } catch (Exception exception) {
            LOG.warn("spark dpp failed for exception:" + exception);
            dppResult.isSuccess = false;
            dppResult.failedReason = exception.getMessage();
            throw exception;
        } finally {
            spark.stop();
            dppResult.normalRows = scannedRowsAcc.value() - abnormalRowAcc.value();
            dppResult.scannedRows = scannedRowsAcc.value();
            dppResult.fileNumber = fileNumberAcc.value();
            dppResult.fileSize = fileSizeAcc.value();
            dppResult.abnormalRows = abnormalRowAcc.value();
            dppResult.partialAbnormalRows = invalidRows.value();
        }
    }

    private void writeDppResult(DppResult dppResult) throws Exception {
        String outputPath = etlJobConfig.getOutputPath();
        String resultFilePath = outputPath + "/" + DPP_RESULT_FILE;
        URI uri = new URI(outputPath);
        FileSystem fs = FileSystem.get(uri, serializableHadoopConf.value());
        Path filePath = new Path(resultFilePath);
        FSDataOutputStream outputStream = fs.create(filePath);
        Gson gson = new Gson();
        outputStream.write(gson.toJson(dppResult).getBytes());
        outputStream.write('\n');
        outputStream.close();
    }

    public void doDpp() throws Exception {
        try {
            process();
        } catch (Exception e) {
            throw e;
        } finally {
            // write dpp result to file in outputPath
            writeDppResult(dppResult);
        }
    }
}

