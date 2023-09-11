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

package org.apache.doris.load.loadv2.etl;

import org.apache.doris.common.SparkDppException;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlTable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.functions;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class SparkLoadConf implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadConf.class);

    private static final String BITMAP_DICT_FUNC = "bitmap_dict";
    private static final String TO_BITMAP_FUNC = "to_bitmap";
    private static final String BITMAP_HASH = "bitmap_hash";
    private static final String BINARY_BITMAP = "binary_bitmap";
    private static final String OUTPUT_FILE_PATTEN = "V1.sl.%d.%d.%d.%d.%d.parquet";

    // private String jobConfigFilePath;
    private EtlJobConfig etlJobConfig;
    private final Set<Long> hiveSourceTables = Sets.newHashSet();
    private final Map<Long, Set<String>> tableToBitmapDictColumns = Maps.newHashMap();
    private final Map<Long, Set<String>> tableToBinaryBitmapColumns = Maps.newHashMap();

    private final SparkLoadCommand command;
    private final SerializableConfiguration hadoopConf;

    private SparkLoadConf(SparkLoadCommand command, SerializableConfiguration hadoopConf) {
        this.command = command;
        this.hadoopConf = hadoopConf;
    }

    public static SparkLoadConf build(SparkLoadCommand command, SerializableConfiguration hadoopConf)
            throws IOException, SparkDppException {

        SparkLoadConf sparkLoadConf = new SparkLoadConf(command, hadoopConf);

        sparkLoadConf.getEtlJobConfigFromFile();
        sparkLoadConf.checkConfig();

        return sparkLoadConf;
    }


    private void getEtlJobConfigFromFile() throws IOException {
        String jsonConfig;
        Path path = new Path(command.getConfigFile());
        try (FileSystem fs = path.getFileSystem(hadoopConf.value()); DataInputStream in = fs.open(path)) {
            jsonConfig = CharStreams.toString(new InputStreamReader(in));
        }
        LOG.info("rdd read json config: " + jsonConfig);
        this.etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
    }

    /*
     * 1. check bitmap column
     * 2. fill tableToBitmapDictColumns
     * 3. remove bitmap_dict and to_bitmap mapping from columnMappings
     */
    private void checkConfig() throws SparkDppException {
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            boolean isHiveSource = false;
            Set<String> bitmapDictColumns = Sets.newHashSet();
            Set<String> binaryBitmapColumns = Sets.newHashSet();

            for (EtlFileGroup fileGroup : entry.getValue().fileGroups) {
                if (fileGroup.sourceType == EtlJobConfig.SourceType.HIVE) {
                    isHiveSource = true;
                }
                Map<String, EtlColumnMapping> newColumnMappings = Maps.newHashMap();
                for (Map.Entry<String, EtlColumnMapping> mappingEntry : fileGroup.columnMappings.entrySet()) {
                    String columnName = mappingEntry.getKey();
                    String exprStr = mappingEntry.getValue().toDescription();
                    String funcName = functions.expr(exprStr).expr().prettyName();

                    switch (funcName.toLowerCase(Locale.ROOT)) {
                        case BINARY_BITMAP:
                            binaryBitmapColumns.add(columnName.toLowerCase());
                            break;
                        case BITMAP_DICT_FUNC:
                            bitmapDictColumns.add(columnName.toLowerCase());
                            break;
                        case TO_BITMAP_FUNC:
                            break;
                        case BITMAP_HASH:
                            throw new SparkDppException("spark load not support " + funcName + " now");
                        default:
                            newColumnMappings.put(mappingEntry.getKey(), mappingEntry.getValue());
                    }
                }

                // reset new columnMappings
                fileGroup.columnMappings = newColumnMappings;
            }
            if (isHiveSource) {
                hiveSourceTables.add(entry.getKey());
            }
            if (!bitmapDictColumns.isEmpty()) {
                tableToBitmapDictColumns.put(entry.getKey(), bitmapDictColumns);
            }
            if (!binaryBitmapColumns.isEmpty()) {
                tableToBinaryBitmapColumns.put(entry.getKey(), binaryBitmapColumns);
            }
        }

        LOG.info("init hiveSourceTables: " + hiveSourceTables
                + ",tableToBitmapDictColumns: " + tableToBitmapDictColumns
                + ",tableToBinaryBitmapColumns: " + tableToBinaryBitmapColumns);

        SparkDppException.checkArgument(hiveSourceTables.size() < 2, "spark etl job must have only one hive table.");
        SparkDppException.checkArgument(tableToBitmapDictColumns.size() < 2,
                "spark etl job must have only one table with bitmap_dict columns");
        SparkDppException.checkArgument(tableToBinaryBitmapColumns.size() < 2,
                "spark etl job must have only one table with bitmap columns");
    }

    public EtlJobConfig getEtlJobConfig() {
        return etlJobConfig;
    }

    public Set<Long> getHiveSourceTables() {
        return hiveSourceTables;
    }

    public Map<Long, EtlTable> getDstTables() {
        return etlJobConfig.tables;
    }

    public Map<Long, Set<String>> getTableToBitmapDictColumns() {
        return tableToBitmapDictColumns;
    }

    public Map<Long, Set<String>> getTableToBinaryBitmapColumns() {
        return tableToBinaryBitmapColumns;
    }

    public SparkLoadCommand getCommend() {
        return command;
    }

    public String getOutputFilePatten() {
        return OUTPUT_FILE_PATTEN;
    }
}
