package org.apache.doris.load.loadv2.dpp.filegroup;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.load.loadv2.dpp.ColumnParser;
import org.apache.doris.load.loadv2.dpp.DppUtils;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import com.google.common.base.Strings;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    abstract public Dataset<Row> loadDataFromFile(String fileUrl) throws SparkDppException;

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
