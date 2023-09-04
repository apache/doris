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

public class SparkLoadFromOrc extends SparkLoadFromFile {
    public SparkLoadFromOrc(SparkLoadSparkEnv loadSparkEnv,
            SparkLoadConf sparkLoadConf,
            EtlIndex baseIndex,
            EtlFileGroup fileGroup,
            StructType dstTableSchema) {
        super(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
    }

    @Override
    public Dataset<Row> loadDataFromFile(String fileUrl) throws SparkDppException {
        List<String> columnValueFromPath = DppUtils.parseColumnsFromPath(fileUrl, fileGroup.columnsFromPath);

        Dataset<Row> dataFrame = spark.read().orc(fileUrl);
        if (!CollectionUtils.isEmpty(columnValueFromPath)) {
            for (int k = 0; k < columnValueFromPath.size(); k++) {
                dataFrame = dataFrame.withColumn(
                        fileGroup.columnsFromPath.get(k), functions.lit(columnValueFromPath.get(k)));
            }
        }
        return dataFrame;
    }
}
