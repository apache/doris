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

package org.apache.doris.load.loadv2.dpp.filegroup;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import org.apache.spark.sql.types.StructType;

import java.util.HashSet;

public class SparkLoadFileGroupFactory {

    public static SparkLoadFileGroup get(
            SparkLoadSparkEnv loadSparkEnv, SparkLoadConf sparkLoadConf, EtlIndex baseIndex, EtlFileGroup fileGroup,
            StructType dstTableSchema, Long tableId) throws SparkDppException {

        switch (fileGroup.sourceType) {
            case FILE:
                switch (fileGroup.fileFormat) {
                    case "parquet":
                        return new SparkLoadFromParquet(
                                loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
                    case "orc":
                        return new SparkLoadFromOrc(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
                    case "csv":
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
