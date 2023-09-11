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

import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public abstract class SparkLoadFromFile extends SparkLoadFileGroup {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadFromFile.class);

    private final List<String> filePaths;

    public SparkLoadFromFile(SparkLoadSparkEnv loadSparkEnv, SparkLoadConf sparkLoadConf,
            EtlIndex baseIndex,
            EtlFileGroup fileGroup,
            StructType dstTableSchema) {
        super(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
        this.filePaths = fileGroup.filePaths;
    }

    abstract Dataset<Row> loadDataFromFile(String fileUrl) throws SparkDppException;

    @Override
    public Dataset<Row> loadDataFromFileGroup() throws Exception {
        return loadDataFromFilePaths();
    }

    private Dataset<Row> loadDataFromFilePaths() throws SparkDppException, IOException {
        Dataset<Row> fileGroupDataframe = null;

        for (String filePath : filePaths) {

            updateAcc(filePath);

            Dataset<Row> dataframe = loadDataFromFile(filePath);

            if (fileGroupDataframe == null) {
                fileGroupDataframe = dataframe;
            } else {
                fileGroupDataframe.union(dataframe);
            }
        }

        if (fileGroupDataframe == null) {
            return null;
        }

        loadSparkEnv.addScannedRowsAcc(fileGroupDataframe.count());

        if (!Strings.isNullOrEmpty(fileGroup.where)) {
            fileGroupDataframe = fileGroupDataframe.where(fileGroup.where);
        }

        return fileGroupDataframe;
    }

    private void updateAcc(String filePath) throws SparkDppException, IOException {

        try {
            FileSystem fs = FileSystem.get(new Path(filePath).toUri(), loadSparkEnv.getHadoopConf());
            FileStatus[] fileStatuses = fs.globStatus(new Path(filePath));
            if (fileStatuses == null) {
                throw new SparkDppException("fs list status failed: " + filePath);
            }
            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isDirectory()) {
                    continue;
                }
                loadSparkEnv.addFileNumberAcc();
                loadSparkEnv.addFileSizeAcc(fileStatus.getLen());
            }
        } catch (Exception e) {
            LOG.error("parse path failed:" + filePath);
            throw e;
        }
    }
}
