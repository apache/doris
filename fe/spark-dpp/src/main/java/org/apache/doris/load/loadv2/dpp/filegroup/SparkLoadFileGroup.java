package org.apache.doris.load.loadv2.dpp.filegroup;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumn;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class SparkLoadFileGroup implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadFileGroup.class);

    public static final String NULL_FLAG = "\\N";
    public static final String BITMAP_TYPE = "bitmap";

    public SparkLoadSparkEnv loadSparkEnv;
    public SparkSession spark;
    EtlIndex baseIndex;
    EtlFileGroup fileGroup;
    StructType dstTableSchema;
    public SparkLoadConf sparkLoadConf;

    public SparkLoadFileGroup(SparkLoadSparkEnv loadSparkEnv, SparkLoadConf sparkLoadConf, EtlIndex baseIndex, EtlFileGroup fileGroup,
            StructType dstTableSchema) {
        this.loadSparkEnv = loadSparkEnv;
        this.sparkLoadConf = sparkLoadConf;
        this.baseIndex = baseIndex;
        this.fileGroup = fileGroup;
        this.dstTableSchema = dstTableSchema;
        this.spark = loadSparkEnv.getSpark();
    }

    public abstract Dataset<Row> loadDataFromFileGroup() throws Exception;

    public Dataset<Row> loadDataFromGroup() throws Exception {
        Dataset<Row> data = loadDataFromFileGroup();
        if (data == null) {
            LOG.info("no data for file file group: " + fileGroup);
            return null;
        }

        data = convertSrcDataframeToDstDataframe(data);

        String debugFileGroupPath = sparkLoadConf.getCommend().getDebugFileGroupPath();
        if (debugFileGroupPath != null) {
            data.write().parquet(debugFileGroupPath);
        }

        return data;
    }

    public Dataset<Row> convertSrcDataframeToDstDataframe(Dataset<Row> srcDataframe) throws SparkDppException {
        Dataset<Row> dataframe = srcDataframe;
        StructType srcSchema = dataframe.schema();
        Set<String> srcColumnNames = new HashSet<>();
        for (StructField field : srcSchema.fields()) {
            srcColumnNames.add(field.name());
        }

        Map<String, EtlColumnMapping> columnMappings = fileGroup.columnMappings;
        // 1. process simple columns
        Set<String> mappingColumns = null;
        if (columnMappings != null) {
            mappingColumns = columnMappings.keySet();
        }

        for (StructField dstField : dstTableSchema.fields()) {
            EtlColumn column = baseIndex.getColumn(dstField.name());
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

}
