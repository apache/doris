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
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.collections.map.MultiValueMap;
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
import org.apache.spark.SparkConf;
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
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.math.BigDecimal;
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

    private static final String DPP_RESULT_FILE = "dpp_result.json";

    private SparkSession spark = null;
    private SparkLoadSparkEnv loadSparkEnv;
    private EtlJobConfig etlJobConfig = null;
    private SparkLoadConf sparkLoadConf;
    private Long specTableId = null;

    private final Map<String, Integer> bucketKeyMap = new HashMap<>();

    private final DppResult dppResult = new DppResult();
    Map<Long, Set<String>> tableToBitmapDictColumns;
    Map<Long, Set<String>> tableToBinaryBitmapColumns;

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
        if (sparkLoadConf.getCommend().getTableId() != null) {
            this.specTableId = sparkLoadConf.getCommend().getTableId();
        }
    }

    private JavaPairRDD<List<Object>, Object[]> processRDDAggregate(JavaPairRDD<List<Object>, Object[]> currentPairRDD,
            RollupTreeNode curNode, SparkRDDAggregator[] sparkRDDAggregators) throws SparkDppException {
        final boolean isDuplicateTable = !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "AGGREGATE")
                && !StringUtils.equalsIgnoreCase(curNode.indexMeta.indexType, "UNIQUE");
        if (!isDuplicateTable) {
            // Aggregate/UNIQUE table
            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    sparkRDDAggregators[idx] = SparkRDDAggregator.buildAggregator(curNode.indexMeta.columns.get(i));
                    idx++;
                }
            }

            if (curNode.indexMeta.isBaseIndex) {
                return currentPairRDD.mapToPair(
                                new EncodeBaseAggregateTableFunction(sparkRDDAggregators))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators));
            } else {
                return currentPairRDD
                        .mapToPair(new EncodeRollupAggregateTableFunction(
                                getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                                        curNode.parent.keyColumnNames, curNode.parent.valueColumnNames)))
                        .reduceByKey(new AggregateReduceFunction(sparkRDDAggregators));
            }
        } else {
            // Duplicate Table
            int idx = 0;
            for (int i = 0; i < curNode.indexMeta.columns.size(); i++) {
                if (!curNode.indexMeta.columns.get(i).isKey) {
                    // duplicate table doesn't need aggregator
                    // init an aggregator here just for keeping interface compatibility when writing data to HDFS
                    sparkRDDAggregators[idx] = new DefaultSparkRDDAggregator();
                    idx++;
                }
            }
            if (curNode.indexMeta.isBaseIndex) {
                return currentPairRDD;
            } else {
                return currentPairRDD
                        .mapToPair(new EncodeRollupAggregateTableFunction(
                                getColumnIndexInParentRollup(curNode.keyColumnNames, curNode.valueColumnNames,
                                curNode.parent.keyColumnNames, curNode.parent.valueColumnNames)));
            }
        }
    }

    // write data to parquet file by using writing the parquet scheme of spark.
    private void writeRepartitionAndSortedRDDToParquet(JavaPairRDD<List<Object>, Object[]> resultRDD,
            String pathPattern, long tableId,
            EtlJobConfig.EtlIndex indexMeta, SparkRDDAggregator[] sparkRDDAggregators) throws IOException {
        // TODO(wb) should deal largeint as BigInteger instead of string when using biginteger as key,
        // data type may affect sorting logic
        StructType dstSchema = DppUtils.convertDorisColumnsToSparkColumns(indexMeta.columns, false, true);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(dstSchema);

        DorisPartitioner partitioner = DorisPartitionFactory.create(bucketKeyMap);
        resultRDD.repartitionAndSortWithinPartitions(partitioner, partitioner)
                .foreachPartition((VoidFunction<Iterator<Tuple2<List<Object>, Object[]>>>) t -> {
                    // write the data to dst file
                    Configuration conf = new Configuration(loadSparkEnv.getHadoopConf());
                    FileSystem fs = FileSystem.get(new Path(sparkLoadConf.getCommend().getOutputPath()).toUri(), conf);
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
                            LOG.error("invalid row:" + pair);
                            continue;
                        }

                        String curBucketKey = keyColumns.get(0).toString();
                        // if the bucket key is new, it will belong to a new tablet
                        if (lastBucketKey == null || !curBucketKey.equals(lastBucketKey)) {
                            if (parquetWriter != null) {
                                parquetWriter.close();
                                // rename tmpPath to path
                                try {
                                    fs.rename(new Path(tmpPath), new Path(dstPath));
                                } catch (IOException ioe) {
                                    LOG.error("rename from tmpPath" + tmpPath + " to dstPath:" + dstPath
                                            + " failed. exception:" + ioe);
                                    throw ioe;
                                }
                            }
                            // flush current writer and create a new writer
                            String[] bucketKey = curBucketKey.split("_");
                            if (bucketKey.length != 2) {
                                LOG.error("invalid bucket key:" + curBucketKey);
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

                        List<Object> columnObjects = new ArrayList<>();
                        for (int i = 1; i < keyColumns.size(); ++i) {
                            columnObjects.add(keyColumns.get(i));
                        }
                        for (int i = 0; i < valueColumns.length; ++i) {
                            columnObjects.add(sparkRDDAggregators[i].finalize(valueColumns[i]));
                        }
                        Row rowWithoutBucketKey = RowFactory.create(columnObjects.toArray());
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
            long tableId, EtlJobConfig.EtlIndex baseIndex) throws SparkDppException, IOException {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Long, JavaPairRDD<List<Object>, Object[]>> parentRDDMap = new HashMap<>();
        parentRDDMap.put(baseIndex.indexId, rootRDD);
        Map<Long, JavaPairRDD<List<Object>, Object[]>> childrenRDDMap = new HashMap<>();
        String pathPattern = sparkLoadConf.getCommend().getOutputPath() + "/" + sparkLoadConf.getOutputFilePatten();
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
    private JavaPairRDD<List<Object>, Object[]> fillTupleWithPartitionColumn(
            Dataset<Row> dataframe,
            EtlJobConfig.EtlPartitionInfo partitionInfo,
            Partitioner partitioner,
            List<String> keyAndPartitionColumnNames, List<String> valueColumnNames, StructType dstTableSchema,
            EtlJobConfig.EtlIndex baseIndex, List<Long> validPartitionIds) throws SparkDppException {

        // generate valid partition index
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

        // use PairFlatMapFunction instead of PairMapFunction because there will be
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
                        LOG.error("invalid partition for row:" + row + ", pid:" + pid);
                        loadSparkEnv.addAbnormalRowAcc();
                        loadSparkEnv.addInvalidRows(row.toString());
                    } else {
                        // TODO(wb) support lagreint for hash
                        long hashValue = DppUtils.getHashValue(
                                row, partitionInfo.distributionColumnRefs, dstTableSchema);
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

    private void process() throws Exception {

        prepareForHiveTable();

        for (Map.Entry<Long, EtlJobConfig.EtlTable> entry : etlJobConfig.tables.entrySet()) {

            Long tableId = entry.getKey();
            EtlJobConfig.EtlTable etlTable = entry.getValue();

            if (specTableId != null && !specTableId.equals(tableId)) {
                continue;
            }

            // get the base index meta
            EtlJobConfig.EtlIndex baseIndex = getEtlIndex(etlTable);
            SparkDppException.checkArgument(baseIndex != null, "should have a base index for: " + tableId);

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

            // get spark struct
            StructType dstTableSchema = DppUtils.convertDorisColumnsToSparkColumns(baseIndex.columns, false, false);
            Set<String> binaryBitmapColumnSet = tableToBinaryBitmapColumns.getOrDefault(tableId, new HashSet<>());
            dstTableSchema = DppUtils.replaceBinaryColsInSchema(binaryBitmapColumnSet, dstTableSchema);

            // solve fileGroups, read all data from source for this table
            JavaPairRDD<List<Object>, Object[]> tablePairRDD = null;
            for (EtlJobConfig.EtlFileGroup fileGroup : etlTable.fileGroups) {

                // load data
                SparkLoadFileGroup sparkLoadFileGroup = SparkLoadFileGroupFactory.get(loadSparkEnv, sparkLoadConf,
                        baseIndex, fileGroup, dstTableSchema, tableId);
                Dataset<Row> fileGroupDataframe = sparkLoadFileGroup.loadDataFromGroup();

                if (fileGroupDataframe == null) {
                    LOG.info("no data for file file group:" + fileGroup);
                    continue;
                }

                // get partitioner
                Partitioner partitioner = DorisPartitionFactory.create(etlTable.partitionInfo, baseIndex.columns,
                        keyAndPartitionColumnNames);
                JavaPairRDD<List<Object>, Object[]> ret = fillTupleWithPartitionColumn(
                        fileGroupDataframe,
                        etlTable.partitionInfo,
                        partitioner,
                        keyAndPartitionColumnNames, valueColumnNames,
                        dstTableSchema, baseIndex, fileGroup.partitions);

                if (tablePairRDD == null) {
                    tablePairRDD = ret;
                } else {
                    tablePairRDD.union(ret);
                }
            }

            // generate rollup table tree
            RollupTreeBuilder rollupTreeParser = new MinimumCoverageRollupTreeBuilder();
            RollupTreeNode rootNode = rollupTreeParser.build(etlTable);

            LOG.info("Start to process rollup tree:" + rootNode);
            processRollupTree(rootNode, tablePairRDD, tableId, baseIndex);
        }

        if (!loadSparkEnv.invalidRowsIsEmpty()) {
            LOG.warn("invalid rows contents:" + loadSparkEnv.getInvalidRowsValue());
        }
    }

    private EtlJobConfig.EtlIndex getEtlIndex(EtlJobConfig.EtlTable etlTable) {
        for (EtlJobConfig.EtlIndex indexMeta : etlTable.indexes) {
            if (indexMeta.isBaseIndex) {
                return indexMeta;
            }
        }
        return null;
    }

    private void writeDppResult() throws Exception {

        dppResult.normalRows = loadSparkEnv.getScannedRowsAccValue() - loadSparkEnv.getAbnormalRowAccValue();
        dppResult.scannedRows = loadSparkEnv.getScannedRowsAccValue();
        dppResult.fileNumber = loadSparkEnv.getFileNumberAccValue();
        dppResult.fileSize = loadSparkEnv.getFileSizeAccValue();
        dppResult.abnormalRows = loadSparkEnv.getAbnormalRowAccValue();
        dppResult.partialAbnormalRows = loadSparkEnv.getInvalidRowsValue();

        String outputPath = sparkLoadConf.getCommend().getOutputPath();
        String resultFilePath = outputPath + "/" + DPP_RESULT_FILE;
        FileSystem fs = FileSystem.get(new Path(outputPath).toUri(), loadSparkEnv.getHadoopConf());
        Path filePath = new Path(resultFilePath);
        FSDataOutputStream outputStream = fs.create(filePath);
        Gson gson = new Gson();
        outputStream.write(gson.toJson(dppResult).getBytes());
        outputStream.write('\n');
        outputStream.close();
    }

    private void prepareForHiveTable() throws SparkDppException {
        if (!sparkLoadConf.getHiveSourceTables().isEmpty()) {
            // only one table
            long tableId = -1;
            EtlTable table = null;
            for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
                tableId = entry.getKey();
                table = entry.getValue();
                break;
            }

            if (specTableId != null && !specTableId.equals(tableId)) {
                return;
            }

            // init hive configs like metastore service
            SparkDppException.checkArgument(table != null, "can't get the hive table");
            EtlFileGroup fileGroup = table.fileGroups.get(0);
            setSparkConfForHive(fileGroup.hiveTableProperties);
            fileGroup.dppHiveDbTableName = fileGroup.hiveDbTableName;

            // build global dict and encode source hive table if it has bitmap dict columns
            if (!tableToBitmapDictColumns.isEmpty() && tableToBitmapDictColumns.containsKey(tableId)) {
                // set with dorisIntermediateHiveDbTable
                fileGroup.dppHiveDbTableName = buildGlobalDictAndEncodeSourceTable(table, tableId);
            }
        }
    }

    private String buildGlobalDictAndEncodeSourceTable(EtlTable table, long tableId) {
        // dict column map
        MultiValueMap dictColumnMap = new MultiValueMap();
        for (String dictColumn : tableToBitmapDictColumns.get(tableId)) {
            dictColumnMap.put(dictColumn, null);
        }

        // doris schema
        List<String> dorisOlapTableColumnList = Lists.newArrayList();
        for (EtlIndex etlIndex : table.indexes) {
            if (etlIndex.isBaseIndex) {
                for (EtlColumn column : etlIndex.columns) {
                    dorisOlapTableColumnList.add(column.columnName);
                }
            }
        }

        // hive db and tables
        EtlFileGroup fileGroup = table.fileGroups.get(0);
        String sourceHiveDBTableName = fileGroup.hiveDbTableName;
        String dorisHiveDB = sourceHiveDBTableName.split("\\.")[0];
        String globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME, tableId);
        String distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME, tableId);
        String dorisIntermediateHiveTable = String.format(
                EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME, tableId);
        String sourceHiveFilter = fileGroup.where;

        // others
        List<String> mapSideJoinColumns = Lists.newArrayList();
        int buildConcurrency = 1;
        List<String> veryHighCardinalityColumn = Lists.newArrayList();
        int veryHighCardinalityColumnSplitNum = 1;

        LOG.info("global dict builder args, dictColumnMap: " + dictColumnMap
                + ", dorisOlapTableColumnList: " + dorisOlapTableColumnList
                + ", sourceHiveDBTableName: " + sourceHiveDBTableName
                + ", sourceHiveFilter: " + sourceHiveFilter
                + ", distinctKeyTableName: " + distinctKeyTableName
                + ", globalDictTableName: " + globalDictTableName
                + ", dorisIntermediateHiveTable: " + dorisIntermediateHiveTable);
        try {
            GlobalDictBuilder globalDictBuilder = new GlobalDictBuilder(dictColumnMap, dorisOlapTableColumnList,
                    mapSideJoinColumns, sourceHiveDBTableName, sourceHiveFilter, dorisHiveDB, distinctKeyTableName,
                    globalDictTableName, dorisIntermediateHiveTable, buildConcurrency, veryHighCardinalityColumn,
                    veryHighCardinalityColumnSplitNum, spark);
            globalDictBuilder.createHiveIntermediateTable();
            globalDictBuilder.extractDistinctColumn();
            globalDictBuilder.buildGlobalDict();
            globalDictBuilder.encodeDorisIntermediateHiveTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return String.format("%s.%s", dorisHiveDB, dorisIntermediateHiveTable);
    }

    private void setSparkConfForHive(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        SparkConf conf = spark.sparkContext().getConf();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
            conf.set("spark.hadoop." + entry.getKey(), entry.getValue());
        }
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
            throw e;
        } finally {
            // write dpp result to file in outputPath
            writeDppResult();
        }
    }
}
