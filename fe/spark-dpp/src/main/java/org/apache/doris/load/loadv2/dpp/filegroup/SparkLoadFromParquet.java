package org.apache.doris.load.loadv2.dpp.filegroup;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.dpp.DppUtils;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public class SparkLoadFromParquet extends SparkLoadFromFile {
    public SparkLoadFromParquet(SparkLoadSparkEnv loadSparkEnv,
            SparkLoadConf sparkLoadConf,
            EtlIndex baseIndex,
            EtlFileGroup fileGroup,
            StructType dstTableSchema) {
        super(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
    }

    @Override
    public Dataset<Row> loadDataFromFile(String fileUrl) throws SparkDppException {
        List<String> columnValueFromPath = DppUtils.parseColumnsFromPath(fileUrl, fileGroup.columnsFromPath);

        // parquet had its own schema, just use it; perhaps we will add some validation in the future.
        Dataset<Row> dataFrame = spark.read().parquet(fileUrl);
        if (!CollectionUtils.isEmpty(columnValueFromPath)) {
            for (int k = 0; k < columnValueFromPath.size(); k++) {
                dataFrame = dataFrame.withColumn(
                        fileGroup.columnsFromPath.get(k), functions.lit(columnValueFromPath.get(k)));
            }
        }
        return dataFrame;
    }

}
