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
import org.apache.doris.load.loadv2.dpp.ColumnParser;
import org.apache.doris.load.loadv2.dpp.DppUtils;
import org.apache.doris.load.loadv2.etl.SparkLoadConf;
import org.apache.doris.load.loadv2.etl.SparkLoadSparkEnv;
import org.apache.doris.sparkdpp.EtlJobConfig;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlFileGroup;
import org.apache.doris.sparkdpp.EtlJobConfig.EtlIndex;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkLoadFromCsv extends SparkLoadFromFile {

    private static final Logger LOG = LoggerFactory.getLogger(SparkLoadFromCsv.class);

    public SparkLoadFromCsv(SparkLoadSparkEnv loadSparkEnv,
            SparkLoadConf sparkLoadConf,
            EtlIndex baseIndex,
            EtlFileGroup fileGroup,
            StructType dstTableSchema) {
        super(loadSparkEnv, sparkLoadConf, baseIndex, fileGroup, dstTableSchema);
    }

    @Override
    public Dataset<Row> loadDataFromFile(String fileUrl) throws SparkDppException {

        List<String> columnValueFromPath = DppUtils.parseColumnsFromPath(fileUrl, fileGroup.columnsFromPath);

        List<String> dataSrcColumns = fileGroup.fileFieldNames;
        if (dataSrcColumns == null) {
            // if there is no source columns info
            // use base index columns as source columns
            dataSrcColumns = new ArrayList<>();
            for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
                dataSrcColumns.add(column.columnName);
            }
        }
        // for getting schema to check source data
        Map<String, Integer> dstColumnNameToIndex = new HashMap<>();
        for (int i = 0; i < baseIndex.columns.size(); i++) {
            dstColumnNameToIndex.put(baseIndex.columns.get(i).columnName, i);
        }
        List<String> srcColumnsWithColumnsFromPath = new ArrayList<>(dataSrcColumns);
        if (fileGroup.columnsFromPath != null) {
            srcColumnsWithColumnsFromPath.addAll(fileGroup.columnsFromPath);
        }

        StructType srcSchema = createScrSchema(srcColumnsWithColumnsFromPath);
        JavaRDD<String> sourceDataRdd = loadSparkEnv.getSpark().read().textFile(fileUrl).toJavaRDD();
        int columnSize = dataSrcColumns.size();
        List<ColumnParser> parsers = new ArrayList<>();
        for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
            parsers.add(ColumnParser.create(column));
        }
        char separator = (char) fileGroup.columnSeparator.getBytes(StandardCharsets.UTF_8)[0];
        JavaRDD<Row> rowRDD = sourceDataRdd.flatMap(
                record -> {
                    String[] attributes = splitLine(record, separator);
                    List<Row> result = new ArrayList<>();
                    boolean validRow = true;
                    if (attributes.length != columnSize) {
                        LOG.warn("invalid src schema, data columns:"
                                + attributes.length + ", file group columns:"
                                + columnSize + ", row:" + record);
                        validRow = false;
                    } else {
                        for (int i = 0; i < attributes.length; ++i) {
                            StructField field = srcSchema.apply(i);
                            String srcColumnName = field.name();
                            if (attributes[i].equals(NULL_FLAG) && dstColumnNameToIndex.containsKey(srcColumnName)) {
                                if (baseIndex.columns.get(dstColumnNameToIndex.get(srcColumnName)).isAllowNull) {
                                    attributes[i] = null;
                                } else {
                                    LOG.warn("column name:" + srcColumnName + ", attribute: " + i
                                            + " can not be null. row:" + record);
                                    validRow = false;
                                    break;
                                }
                            }

                            boolean strictMode = sparkLoadConf.getCommend().getStrictMode();
                            if (strictMode) {
                                if (dstColumnNameToIndex.containsKey(srcColumnName)) {
                                    int index = dstColumnNameToIndex.get(srcColumnName);
                                    String type = baseIndex.columns.get(index).columnType;
                                    if (type.equalsIgnoreCase("CHAR")
                                            || type.equalsIgnoreCase("VARCHAR")
                                            || fileGroup.columnMappings.containsKey(field.name())) {
                                        continue;
                                    }
                                    ColumnParser parser = parsers.get(index);
                                    boolean valid = parser.parse(attributes[i]);
                                    if (!valid) {
                                        validRow = false;
                                        LOG.warn("invalid row:" + record
                                                + ", attribute " + i + ": " + attributes[i] + " parsed failed");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if (validRow) {
                        Row row = null;
                        if (fileGroup.columnsFromPath == null) {
                            row = RowFactory.create(attributes);
                        } else {
                            // process columns from path
                            // append columns from path to the tail
                            List<String> columnAttributes = new ArrayList<>();
                            columnAttributes.addAll(Arrays.asList(attributes));
                            columnAttributes.addAll(columnValueFromPath);
                            row = RowFactory.create(columnAttributes.toArray());
                        }
                        result.add(row);
                    } else {
                        loadSparkEnv.addAbnormalRowAcc();
                        loadSparkEnv.addInvalidRows(record);
                    }
                    return result.iterator();
                }
        );

        return spark.createDataFrame(rowRDD, srcSchema);
    }

    private StructType createScrSchema(List<String> srcColumns) {
        List<StructField> fields = new ArrayList<>();
        for (String srcColumn : srcColumns) {
            // user StringType to load source data
            StructField field = DataTypes.createStructField(srcColumn, DataTypes.StringType, true);
            fields.add(field);
        }
        return DataTypes.createStructType(fields);
    }

    // This method is to keep the splitting consistent with broker load / mini load
    private String[] splitLine(String line, char sep) {
        if (line == null || line.equals("")) {
            return new String[0];
        }
        int index = 0;
        int lastIndex = 0;
        // line-begin char and line-end char are considered to be 'delimiter'
        List<String> values = new ArrayList<>();
        for (int i = 0; i < line.length(); i++, index++) {
            if (line.charAt(index) == sep) {
                values.add(line.substring(lastIndex, index));
                lastIndex = index + 1;
            }
        }
        values.add(line.substring(lastIndex, index));
        return values.toArray(new String[0]);
    }

}
