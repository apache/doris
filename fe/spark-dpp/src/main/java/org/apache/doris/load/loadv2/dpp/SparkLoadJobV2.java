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

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.dpp.filegroup.SparkLoadFileGroup;
import org.apache.doris.load.loadv2.dpp.filegroup.SparkLoadFileGroupFactory;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.DppResult;
import org.apache.doris.sparkdpp.EtlJobConfig;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.Partitioner;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

public final class SparkLoadJobV2 implements java.io.Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadJobV2.class);

    private static final String NULL_FLAG = "\\N";
    private static final String DPP_RESULT_FILE = "dpp_result.json";
    private static final String BITMAP_TYPE = "bitmap";

    private SparkSession spark = null;
    private SparkLoadSparkEnv loadSparkEnv;
    private EtlJobConfig etlJobConfig = null;

    private Map<String, Integer> bucketKeyMap = new HashMap<>();

    private DppResult dppResult = new DppResult();
    Map<Long, Set<String>> tableToBitmapDictColumns;
    Map<Long, Set<String>> tableToBinaryBitmapColumns;

    SparkLoadConf sparkLoadConf;

    // just for ut
    public SparkLoadJobV2() {
    }

    public SparkLoadJobV2(SparkLoadSparkEnv loadSparkEnv, SparkLoadConf sparkLoadConf) {
        this.loadSparkEnv = loadSparkEnv;
        this.spark = loadSparkEnv.getSpark();
        this.sparkLoadConf = sparkLoadConf;
        this.etlJobConfig = sparkLoadConf.getEtlJobConfig();
        this.tableToBitmapDictColumns = sparkLoadConf.getTableToBitmapDictColumns();
        this.tableToBinaryBitmapColumns = sparkLoadConf.getTableToBinaryBitmapColumns();
    }

    private JavaPairRDD<List<Object>, Object[]> processRDDAggregate(JavaPairRDD<List<Object>, Object[]> currentPairRDD,
            RollupTreeNode curNode, SparkRDDAggregator[] sparkRDDAggregators) throws SparkDppException {
        final boolean isDuplicateTable = !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "AGGREGATE")
                && !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "UNIQUE");
        // Aggregate/UNIQUE table
        if (!isDuplicateTable) {
            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    sparkRDDAggregators[idx] = SparkRDDAggregator.buildAggregator(curNode.indexMeta.columns.get(i));
                    idx++;
                }
            }

            if (curNode.indexMeta.isBaseIndex) {
                JavaPairRDD<List<Object>, Object[]> result = currentPairRDD.mapToPair(
                                new EncodeBaseAggregateTableFunction(sparkRDDAggregators))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators));
                return result;
            } else {
                JavaPairRDD<List<Object>, Object[]> result = currentPairRDD
                        .mapToPair(new EncodeRollupAggregateTableFunction(
                                getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                                        curNode.parent.keyColumnNames, curNode.parent.valueColumnNames)))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators));
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
            String pathPattern, long tableId,
            EtlJobConfig.EtlIndex indexMeta, SparkRDDAggregator[] sparkRDDAggregators) {
        // TODO(wb) should deal largeint as BigInteger instead of string when using biginteger as key,
        // data type may affect sorting logic
        StructType dstSchema = DppUtils.createDstTableSchema(indexMeta.columns, false, true);
        ExpressionEncoder encoder = RowEncoder.apply(dstSchema);

        resultRDD.repartitionAndSortWithinPartitions(new BucketPartitioner(bucketKeyMap), new BucketComparator())
                .foreachPartition((VoidFunction<Iterator<Tuple2<List<Object>, Object[]>>>) t -> {
                    // write the data to dst file
                    Configuration conf = new Configuration(loadSparkEnv.getHadoopConf());
                    FileSystem fs = FileSystem.get(new Path(etlJobConfig.outputPath).toUri(), conf);
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
                                    LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath
                                            + " failed. exception:" + ioe);
                                    throw ioe;
                                }
                            }
                            // flush current writer and create a new writer
                            String[] bucketKey = curBucketKey.split("_");
                            if (bucketKey.length != 2) {
                                LOG.warn("invalid bucket key:" + curBucketKey);
                                continue;
                            }
                            long partitionId = Long.parseLong(bucketKey[0]);
                            int bucketId = Integer.parseInt(bucketKey[1]);
                            dstPath = String.format(pathPattern, tableId, partitionId, indexMeta.indexId, bucketId,
                                    indexMeta.schemaHash);
                            tmpPath = dstPath + "." + taskAttemptId;
                            conf.setBoolean("spark.sql.parquet.writeLegacyFormat", false);
                            conf.setBoolean("spark.sql.parquet.int64AsTimestampMillis", false);
                            conf.setBoolean("spark.sql.parquet.int96AsTimestamp", true);
                            conf.setBoolean("spark.sql.parquet.binaryAsString", false);
                            conf.set("spark.sql.parquet.outputTimestampType", "INT96");
                            ParquetWriteSupport.setSchema(dstSchema, conf);
                            ParquetWriteSupport parquetWriteSupport = new ParquetWriteSupport();
                            parquetWriter = new ParquetWriter<>(new Path(tmpPath), parquetWriteSupport,
                                    CompressionCodecName.SNAPPY, 256 * 1024 * 1024, 16 * 1024, 1024 * 1024, true, false,
                                    WriterVersion.PARQUET_1_0, conf);
                            if (parquetWriter != null) {
                                LOG.info("[HdfsOperate]>> initialize writer succeed! path:" + tmpPath);
                            }
                            lastBucketKey = curBucketKey;
                        }
                        // TODO wuwenchi xxxx toRow 不兼容，3.2 版本没这个方法了
                        // https://blog.csdn.net/yyoc97/article/details/109273555
                        // 这里只是为了一个分区写一个文件。
                        // 那么可以先把分区的rdd转换成组装好的columnObjects
                        // 然后然后通过RDD转成dataframe，collect后写入parquet
                        InternalRow internalRow = encoder.toRow(rowWithoutBucketKey);
                        parquetWriter.write(internalRow);
                    }
                    if (parquetWriter != null) {
                        parquetWriter.close();
                        try {
                            fs.rename(new Path(tmpPath), new Path(dstPath));
                        } catch (IOException ioe) {
                            LOG.warn("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath + " failed. exception:"
                                    + ioe);
                            throw ioe;
                        }
                    }

                });
    }

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
    private Pair<Integer[], Integer[]> getColumnIndexInParentRollup(List<String> childRollupKeyColumns,
            List<String> childRollupValueColumns, List<String> parentRollupKeyColumns,
            List<String> parentRollupValueColumns) throws SparkDppException {
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
            throw new SparkDppException(String.format("column map index from child to parent has error,"
                            + " key size src: %s, dst: %s; value size src: %s, dst: %s",
                    childRollupKeyColumns.size(), keyMap.size(), childRollupValueColumns.size(), valueMap.size()));
        }

        return Pair.of(keyMap.toArray(new Integer[keyMap.size()]), valueMap.toArray(new Integer[valueMap.size()]));
    }

    /**
     * check decimal,char/varchar
     */
    public boolean validateData(Object srcValue, EtlJobConfig.EtlColumn etlColumn, ColumnParser columnParser, Row row) {

        switch (etlColumn.columnType.toUpperCase()) {
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128":
                // TODO(wb):  support decimal round; see be DecimalV2Value::round
                DecimalParser decimalParser = (DecimalParser) columnParser;
                BigDecimal srcBigDecimal = (BigDecimal) srcValue;
                if (srcValue != null && (decimalParser.getMaxValue().compareTo(srcBigDecimal) < 0
                        || decimalParser.getMinValue().compareTo(srcBigDecimal) > 0)) {
                    LOG.warn(String.format("decimal value is not valid for defination, column=%s,"
                                    + " value=%s,precision=%s,scale=%s",
                            etlColumn.columnName, srcValue, srcBigDecimal.precision(), srcBigDecimal.scale()));
                    return false;
                }
                break;
            case "CHAR":
            case "VARCHAR":
                // TODO(wb) padding char type
                int strSize = 0;
                if (srcValue != null && (strSize = srcValue.toString().getBytes(StandardCharsets.UTF_8).length)
                        > etlColumn.stringLength) {
                    LOG.warn(String.format("the length of input is too long than schema."
                                    + " column_name:%s,input_str[%s],schema length:%s,actual length:%s",
                            etlColumn.columnName, row.toString(), etlColumn.stringLength, strSize));
                    return false;
                }
                break;
            case "STRING":
            case "TEXT":
                // TODO(zjf) padding string type
                int strDataSize = 0;
                if (srcValue != null && (strDataSize = srcValue.toString().getBytes(StandardCharsets.UTF_8).length)
                        > DppUtils.STRING_LENGTH_LIMIT) {
                    LOG.warn(String.format("The string type is limited to a maximum of %s bytes."
                                    + " column_name:%s,input_str[%s],actual length:%s",
                            DppUtils.STRING_LENGTH_LIMIT, etlColumn.columnName, row.toString(), strDataSize));
                    return false;
                }
                break;
            default:
                return true;
        }
        return true;
    }

    /**
     * 1 project column and reorder column
     * 2 validate data
     * 3 fill tuple with partition column
     */
    private JavaPairRDD<List<Object>, Object[]> fillTupleWithPartitionColumn(Dataset<Row> dataframe,
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Integer> partitionKeyIndex,
            List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys,
            List<String> keyAndPartitionColumnNames, List<String> valueColumnNames, StructType dstTableSchema,
            EtlJobConfig.EtlIndex baseIndex, List<Long> validPartitionIds) throws SparkDppException {
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

        Map<String, ColumnParser> parsers = Maps.newHashMap();
        for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
            parsers.put(column.columnName, ColumnParser.create(column));
        }

        // use PairFlatMapFunction instead of PairMapFunction because the there will be
        // 0 or 1 output row for 1 input row
        JavaPairRDD<List<Object>, Object[]> resultPairRDD = dataframe.toJavaRDD().flatMapToPair(
                (PairFlatMapFunction<Row, List<Object>, Object[]>) row -> {
                    List<Tuple2<List<Object>, Object[]>> result = new ArrayList<>();
                    List<Object> keyAndPartitionColumns = new ArrayList<>();
                    List<Object> keyColumns = new ArrayList<>();
                    List<Object> valueColumns = new ArrayList<>(valueColumnNames.size());
                    for (int i = 0; i < keyAndPartitionColumnNames.size(); i++) {
                        String columnName = keyAndPartitionColumnNames.get(i);
                        Object columnObject = row.get(row.fieldIndex(columnName));
                        if (!validateData(columnObject, baseIndex.getColumn(columnName),
                                parsers.get(columnName), row)) {
                            loadSparkEnv.addAbnormalRowAcc();
                            return result.iterator();
                        }
                        keyAndPartitionColumns.add(columnObject);

                        if (baseIndex.getColumn(columnName).isKey) {
                            keyColumns.add(columnObject);
                        }
                    }

                    for (int i = 0; i < valueColumnNames.size(); i++) {
                        String columnName = valueColumnNames.get(i);
                        Object columnObject = row.get(row.fieldIndex(columnName));
                        if (!validateData(columnObject, baseIndex.getColumn(columnName),
                                parsers.get(columnName), row)) {
                            loadSparkEnv.addAbnormalRowAcc();
                            return result.iterator();
                        }
                        valueColumns.add(columnObject);
                    }

                    DppColumns key = new DppColumns(keyAndPartitionColumns);
                    int pid = partitioner.getPartition(key);
                    if (!validPartitionIndex.contains(pid)) {
                        LOG.warn("invalid partition for row:" + row + ", pid:" + pid);
                        loadSparkEnv.addAbnormalRowAcc();
                        loadSparkEnv.addInvalidRows(row.toString());
                    } else {
                        // TODO(wb) support lagreint for hash
                        long hashValue = DppUtils.getHashValue(row, distributeColumns, dstTableSchema);
                        int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
                        long partitionId = partitionInfo.partitions.get(pid).partitionId;
                        // bucketKey is partitionId_bucketId
                        String bucketKey = partitionId + "_" + bucketId;

                        List<Object> tuple = new ArrayList<>();
                        tuple.add(bucketKey);
                        tuple.addAll(keyColumns);
                        result.add(new Tuple2<>(tuple, valueColumns.toArray()));
                    }
                    return result.iterator();
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
            Dataset<Row> srcDataframe, StructType dstTableSchema, EtlJobConfig.EtlFileGroup fileGroup)
            throws SparkDppException {
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
                dataframe = dataframe.withColumn(dstField.name(),
                        dataframe.col(dstField.name()).cast(DataTypes.DateType));
            } else if (column.columnType.equalsIgnoreCase("DATETIME")) {
                dataframe = dataframe.withColumn(dstField.name(),
                        dataframe.col(dstField.name()).cast(DataTypes.TimestampType));
            } else if (column.columnType.equalsIgnoreCase("BOOLEAN")) {
                dataframe = dataframe.withColumn(dstField.name(),
                        functions.when(functions.lower(dataframe.col(dstField.name())).equalTo("true"), "1")
                                .when(dataframe.col(dstField.name()).equalTo("1"), "1")
                                .otherwise("0"));
            } else if (!column.columnType.equalsIgnoreCase(BITMAP_TYPE)
                    && !dstField.dataType().equals(DataTypes.StringType)) {
                dataframe = dataframe.withColumn(dstField.name(),
                        dataframe.col(dstField.name()).cast(dstField.dataType()));
            } else if (column.columnType.equalsIgnoreCase(BITMAP_TYPE)
                    && dstField.dataType().equals(DataTypes.BinaryType)) {
                dataframe = dataframe.withColumn(dstField.name(),
                        dataframe.col(dstField.name()).cast(DataTypes.BinaryType));
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
        return dataframe;
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
                // TODO(wb) gson will cast origin value to double by default
                // when the partition column is largeint, this will cause error data
                // need fix it thoroughly
                return new BigInteger(srcValue.toString());
            } else if (dstClass.equals(java.sql.Date.class) || dstClass.equals(java.util.Date.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDate((int) srcValueDouble);
            } else if (dstClass.equals(java.sql.Timestamp.class)) {
                double srcValueDouble = (double) srcValue;
                return convertToJavaDatetime((long) srcValueDouble);
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

    private void process() throws Exception {
        for (Map.Entry<Long, EtlJobConfig.EtlTable> entry : etlJobConfig.tables.entrySet()) {
            Long tableId = entry.getKey();
            EtlJobConfig.EtlTable etlTable = entry.getValue();
            Set<String> binaryBitmapColumnSet = tableToBinaryBitmapColumns.getOrDefault(tableId, new HashSet<>());

            // get the base index meta
            EtlJobConfig.EtlIndex baseIndex = null;
            for (EtlJobConfig.EtlIndex indexMeta : etlTable.indexes) {
                if (indexMeta.isBaseIndex) {
                    baseIndex = indexMeta;
                    break;
                }
            }

            // get key and partition column names and value column names separately
            List<String> keyAndPartitionColumnNames = new ArrayList<>();
            List<String> valueColumnNames = new ArrayList<>();
            for (EtlJobConfig.EtlColumn etlColumn : baseIndex.columns) {
                if (etlColumn.isKey) {
                    keyAndPartitionColumnNames.add(etlColumn.columnName);
                } else {
                    if (etlTable.partitionInfo.partitionColumnRefs.contains(etlColumn.columnName)) {
                        keyAndPartitionColumnNames.add(etlColumn.columnName);
                    }
                    valueColumnNames.add(etlColumn.columnName);
                }
            }

            EtlJobConfig.EtlPartitionInfo partitionInfo = etlTable.partitionInfo;
            List<Integer> partitionKeyIndex = new ArrayList<>();
            List<Class> partitionKeySchema = new ArrayList<>();
            for (String key : partitionInfo.partitionColumnRefs) {
                for (int i = 0; i < baseIndex.columns.size(); ++i) {
                    EtlJobConfig.EtlColumn column = baseIndex.columns.get(i);
                    if (column.columnName.equals(key)) {
                        partitionKeyIndex.add(keyAndPartitionColumnNames.indexOf(key));
                        partitionKeySchema.add(DppUtils.getClassFromColumn(column));
                        break;
                    }
                }
            }

            List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys
                    = createPartitionRangeKeys(partitionInfo, partitionKeySchema);
            StructType dstTableSchema = DppUtils.createDstTableSchema(baseIndex.columns, false, false);
            dstTableSchema = DppUtils.replaceBinaryColsInSchema(binaryBitmapColumnSet, dstTableSchema);

            JavaPairRDD<List<Object>, Object[]> tablePairRDD = null;
            for (EtlJobConfig.EtlFileGroup fileGroup : etlTable.fileGroups) {

                SparkLoadFileGroup sparkLoadFileGroup = SparkLoadFileGroupFactory.get(loadSparkEnv, sparkLoadConf,
                        baseIndex, fileGroup, dstTableSchema, tableId);
                Dataset<Row> fileGroupDataframe = sparkLoadFileGroup.loadDataFromGroup();

                if (fileGroupDataframe == null) {
                    LOG.info("no data for file file group:" + fileGroup);
                    continue;
                }

                JavaPairRDD<List<Object>, Object[]> ret = fillTupleWithPartitionColumn(
                        fileGroupDataframe,
                        partitionInfo, partitionKeyIndex,
                        partitionRangeKeys,
                        keyAndPartitionColumnNames, valueColumnNames,
                        dstTableSchema, baseIndex, fileGroup.partitions);
                if (tablePairRDD == null) {
                    tablePairRDD = ret;
                } else {
                    tablePairRDD.union(ret);
                }
            }

            RollupTreeBuilder rollupTreeParser = new MinimumCoverageRollupTreeBuilder();
            RollupTreeNode rootNode = rollupTreeParser.build(etlTable);
            LOG.info("Start to process rollup tree:" + rootNode);
            processRollupTree(rootNode, tablePairRDD, tableId, baseIndex);
        }
        LOG.info("invalid rows contents:" + loadSparkEnv.getInvalidRowsValue());
    }

    private void writeDppResult() throws Exception {

        dppResult.normalRows = loadSparkEnv.getScannedRowsAccValue() - loadSparkEnv.getAbnormalRowAccValue();
        dppResult.scannedRows = loadSparkEnv.getScannedRowsAccValue();
        dppResult.fileNumber = loadSparkEnv.getFileNumberAccValue();
        dppResult.fileSize = loadSparkEnv.getFileSizeAccValue();
        dppResult.abnormalRows = loadSparkEnv.getAbnormalRowAccValue();
        dppResult.partialAbnormalRows = loadSparkEnv.getInvalidRowsValue();

        String outputPath = etlJobConfig.outputPath;
        String resultFilePath = outputPath + "/" + DPP_RESULT_FILE;
        FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), loadSparkEnv.getHadoopConf());
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

            dppResult.isSuccess = true;
            dppResult.failedReason = "";
        } catch (Throwable e) {
            dppResult.isSuccess = false;
            dppResult.failedReason = e.getMessage();
            LOG.error("spark dpp failed for exception: ", e);
        } finally {
            // write dpp result to file in outputPath
            writeDppResult();
            spark.stop();
        }
    }
}
