package org.apache.doris.load.loadv2.dpp.filegroup;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.dpp.ColumnParser;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class SparkLoadFromHive extends SparkLoadFileGroup {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadFromHive.class);

    private Set<String> dictBitmapColumnSet;
    private Set<String> binaryBitmapColumnSet;

    public SparkLoadFromHive(SparkLoadSparkEnv loadSparkEnv, SparkLoadConf sparkLoadConf,
            EtlIndex baseIndex,
            EtlFileGroup fileGroup,
            StructType dstTableSchema,
            Set<String> dictBitmapColumnSet,
            Set<String> binaryBitmapColumnSet) {
        super(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
        this.dictBitmapColumnSet = dictBitmapColumnSet;
        this.binaryBitmapColumnSet = binaryBitmapColumnSet;
    }

    @Override
    public Dataset<Row> loadDataFromFileGroup() throws Exception {
        return loadDataFromHiveTable();
    }

    private Dataset<Row> loadDataFromHiveTable() throws SparkDppException {
        // select base index columns from hive table
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        baseIndex.columns.forEach(column -> sql.append(column.columnName).append(","));
        sql.deleteCharAt(sql.length() - 1).append(" from ").append(fileGroup.dppHiveDbTableName);
        if (!Strings.isNullOrEmpty(fileGroup.where)) {
            sql.append(" where ").append(fileGroup.where);
        }

        Dataset<Row> dataframe = spark.sql(sql.toString());
        // Note(wb): in current spark load implementation, spark load can't be consistent with doris BE;
        // The reason is as follows
        // For stream load in doris BE, it runs as follows steps:
        // step 1: type check
        // step 2: expression calculation
        // step 3: strict mode check
        // step 4: nullable column check
        // BE can do the four steps row by row
        // but spark load relies on spark to do step2, so it can only do step 1 for whole dataset
        // and then do step 2 for whole dataset and so on;
        // So in spark load, we first do step 1,3,4,and then do step 2.
        dataframe = checkDataFromHiveWithStrictMode(dataframe, baseIndex, fileGroup.columnMappings.keySet(),
                sparkLoadConf.getEtlJobConfig().properties.strictMode, dstTableSchema, dictBitmapColumnSet, binaryBitmapColumnSet);
        return dataframe;
    }

    private Dataset<Row> checkDataFromHiveWithStrictMode(Dataset<Row> dataframe, EtlJobConfig.EtlIndex baseIndex,
            Set<String> mappingColKeys, boolean isStrictMode, StructType dstTableSchema,
            Set<String> dictBitmapColumnSet, Set<String> binaryBitmapColumnsSet) throws SparkDppException {
        List<EtlColumn> columnNameNeedCheckArrayList = new ArrayList<>();
        List<ColumnParser> columnParserArrayList = new ArrayList<>();
        for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
            // note(wb): there are three data source for bitmap column
            // case 1: global dict and binary data; needn't check
            // case 2: bitmap hash function; this func is not supported in spark load now, so ignore it here
            // case 3: origin value is a integer value; it should be checked use LongParser
            if (StringUtils.equalsIgnoreCase(column.columnType, "bitmap")) {
                if (dictBitmapColumnSet.contains(column.columnName.toLowerCase())) {
                    continue;
                }
                if (binaryBitmapColumnsSet.contains(column.columnName.toLowerCase())) {
                    continue;
                }
                columnNameNeedCheckArrayList.add(column);
                // TODO  wuwenchi xxxx 这里的 BigIntParser 应该怎么弄出来呢？
                // columnParserArrayList.add(new BigIntParser());
                EtlColumn etlColumn = new EtlColumn();
                etlColumn.columnType = "BIGINT";
                columnParserArrayList.add(ColumnParser.create(etlColumn));
            } else if (!StringUtils.equalsIgnoreCase(column.columnType, "varchar")
                    && !StringUtils.equalsIgnoreCase(column.columnType, "char")
                    && !mappingColKeys.contains(column.columnName)) {
                columnNameNeedCheckArrayList.add(column);
                columnParserArrayList.add(ColumnParser.create(column));
            }
        }

        ColumnParser[] columnParserArray = columnParserArrayList.toArray(new ColumnParser[0]);
        EtlJobConfig.EtlColumn[] columnNameArray = columnNameNeedCheckArrayList.toArray(new EtlJobConfig.EtlColumn[0]);

        StructType srcSchema = dataframe.schema();
        JavaRDD<Row> result = dataframe.toJavaRDD().flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row row) throws Exception {
                List<Row> result = new ArrayList<>();
                Set<Integer> columnIndexNeedToRepalceNull = new HashSet<Integer>();
                boolean validRow = true;
                for (int i = 0; i < columnNameArray.length; i++) {
                    EtlJobConfig.EtlColumn column = columnNameArray[i];
                    int fieldIndex = row.fieldIndex(column.columnName);
                    Object value = row.get(fieldIndex);
                    if (value == null && !column.isAllowNull) {
                        validRow = false;
                        LOG.warn("column:" + i + " can not be null. row:" + row.toString());
                        break;
                    }
                    if (value != null && !columnParserArray[i].parse(value.toString())) {
                        if (isStrictMode) {
                            validRow = false;
                            LOG.warn(String.format("row parsed failed in strict mode, column name %s, src row %s",
                                    column.columnName, row.toString()));
                        } else if (!column.isAllowNull) {
                            // a column parsed failed would be filled null,
                            // but if doris column is not allowed null, we should skip this row
                            validRow = false;
                            LOG.warn("column:" + i + " can not be null. row:" + row.toString());
                            break;
                        } else {
                            columnIndexNeedToRepalceNull.add(fieldIndex);
                        }
                    }
                }
                if (!validRow) {
                    loadSparkEnv.addAbnormalRowAcc();
                    loadSparkEnv.addInvalidRows(row.toString());
                } else if (columnIndexNeedToRepalceNull.size() != 0) {
                    Object[] newRow = new Object[row.size()];
                    for (int i = 0; i < row.size(); i++) {
                        if (columnIndexNeedToRepalceNull.contains(i)) {
                            newRow[i] = null;
                        } else {
                            newRow[i] = row.get(i);
                        }
                    }
                    result.add(RowFactory.create(newRow));
                } else {
                    result.add(row);
                }
                return result.iterator();
            }
        });

        // here we just check data but not do cast,
        // so data type should be same with src schema which is hive table schema
        return spark.createDataFrame(result, srcSchema);
    }
}
