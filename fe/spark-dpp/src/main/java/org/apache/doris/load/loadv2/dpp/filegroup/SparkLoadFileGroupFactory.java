package org.apache.doris.load.loadv2.dpp.filegroup;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import org.apache.spark.sql.types.StructType;

import java.util.HashSet;

public class SparkLoadFileGroupFactory {

    static public SparkLoadFileGroup get(
            SparkLoadSparkEnv loadSparkEnv, SparkLoadConf sparkLoadConf, EtlIndex baseIndex, EtlFileGroup fileGroup,
            StructType dstTableSchema, Long tableId) throws SparkDppException {

        switch (fileGroup.sourceType) {
            case FILE:
                switch (fileGroup.fileFormat) {
                    case PARQUET:
                        return new SparkLoadFromParquet(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
                    case ORC:
                        return new SparkLoadFromOrc(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
                    case CSV:
                        if (fileGroup.columnSeparator == null) {
                            throw new SparkDppException("Reason: invalid null column separator!");
                        }
                        return new SparkLoadFromCsv(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
                    default:
                        throw new SparkDppException("Not supported file format for [" + fileGroup.fileFormat + "].");
                }

            case HIVE:
                return new SparkLoadFromHive(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema,
                        sparkLoadConf.getTableToBitmapDictColumns().getOrDefault(tableId, new HashSet<>()),
                        sparkLoadConf.getTableToBinaryBitmapColumns().getOrDefault(tableId, new HashSet<>()));

            default:
                throw new SparkDppException("Not supported source type for [" + fileGroup.sourceType + "].");
        }
    }
}