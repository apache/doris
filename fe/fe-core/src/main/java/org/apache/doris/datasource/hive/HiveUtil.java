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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Hive util for create or query hive table.
 */
public final class HiveUtil {

    private HiveUtil() {
    }

    /**
     * get input format class from inputFormatName.
     *
     * @param jobConf         jobConf used when getInputFormatClass
     * @param inputFormatName inputFormat class name
     * @param symlinkTarget   use target inputFormat class when inputFormat is SymlinkTextInputFormat
     * @return a class of inputFormat.
     * @throws UserException when class not found.
     */
    public static InputFormat<?, ?> getInputFormat(JobConf jobConf,
                                                   String inputFormatName, boolean symlinkTarget) throws UserException {
        try {
            Class<? extends InputFormat<?, ?>> inputFormatClass = getInputFormatClass(jobConf, inputFormatName);
            if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }

            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        } catch (ClassNotFoundException | RuntimeException e) {
            throw new UserException("Unable to create input format " + inputFormatName, e);
        }
    }

    @SuppressWarnings({"unchecked", "RedundantCast"})
    private static Class<? extends InputFormat<?, ?>> getInputFormatClass(JobConf conf, String inputFormatName)
            throws ClassNotFoundException {
        // CDH uses different names for Parquet
        if ("parquet.hive.DeprecatedParquetInputFormat".equals(inputFormatName)
                || "parquet.hive.MapredParquetInputFormat".equals(inputFormatName)) {
            return MapredParquetInputFormat.class;
        }

        Class<?> clazz = conf.getClassByName(inputFormatName);
        return (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
    }

    public static boolean isSplittable(RemoteFileSystem remoteFileSystem, String inputFormat,
            String location, JobConf jobConf) throws UserException {
        if (remoteFileSystem instanceof BrokerFileSystem) {
            return ((BrokerFileSystem) remoteFileSystem).isSplittable(location, inputFormat);
        }

        // All supported hive input format are splittable
        return HMSExternalTable.SUPPORTED_HIVE_FILE_FORMATS.contains(inputFormat);
    }

    public static String getHivePartitionValue(String part) {
        String[] kv = part.split("=");
        Preconditions.checkState(kv.length == 2, String.format("Malformed partition name %s", part));
        try {
            // hive partition value maybe contains special characters like '=' and '/'
            return URLDecoder.decode(kv[1], StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // It should not be here
            throw new RuntimeException(e);
        }
    }

    // "c1=a/c2=b/c3=c" ---> List("a","b","c")
    public static List<String> toPartitionValues(String partitionName) {
        ImmutableList.Builder<String> resultBuilder = ImmutableList.builder();
        int start = 0;
        while (true) {
            while (start < partitionName.length() && partitionName.charAt(start) != '=') {
                start++;
            }
            start++;
            int end = start;
            while (end < partitionName.length() && partitionName.charAt(end) != '/') {
                end++;
            }
            if (start > partitionName.length()) {
                break;
            }
            resultBuilder.add(FileUtils.unescapePathName(partitionName.substring(start, end)));
            start = end + 1;
        }
        return resultBuilder.build();
    }

    // List("c1=a/c2=b/c3=c", "c1=a/c2=b/c3=d")
    //           |
    //           |
    //           v
    // Map(
    //      key:"c1=a/c2=b/c3=c", value:Partition(values=List(a,b,c))
    //      key:"c1=a/c2=b/c3=d", value:Partition(values=List(a,b,d))
    //    )
    public static Map<String, Partition> convertToNamePartitionMap(
            List<String> partitionNames,
            List<Partition> partitions) {

        Map<String, List<String>> partitionNameToPartitionValues =
                partitionNames
                    .stream()
                    .collect(Collectors.toMap(partitionName -> partitionName, HiveUtil::toPartitionValues));

        Map<List<String>, Partition> partitionValuesToPartition =
                partitions.stream()
                    .collect(Collectors.toMap(Partition::getValues, partition -> partition));

        ImmutableMap.Builder<String, Partition> resultBuilder = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> entry : partitionNameToPartitionValues.entrySet()) {
            Partition partition = partitionValuesToPartition.get(entry.getValue());
            resultBuilder.put(entry.getKey(), partition);
        }
        return resultBuilder.build();
    }

    public static Table toHiveTable(HiveTableMetadata hiveTable) {
        Objects.requireNonNull(hiveTable.getDbName(), "Hive database name should be not null");
        Objects.requireNonNull(hiveTable.getTableName(), "Hive table name should be not null");
        Table table = new Table();
        table.setDbName(hiveTable.getDbName());
        table.setTableName(hiveTable.getTableName());
        // table.setOwner("");
        int createTime = (int) System.currentTimeMillis() * 1000;
        table.setCreateTime(createTime);
        table.setLastAccessTime(createTime);
        // table.setRetention(0);
        String location = hiveTable.getProperties().get(HiveMetadataOps.LOCATION_URI_KEY);
        Set<String> partitionSet = new HashSet<>(hiveTable.getPartitionKeys());
        Pair<List<FieldSchema>, List<FieldSchema>> hiveSchema = toHiveSchema(hiveTable.getColumns(), partitionSet);

        table.setSd(toHiveStorageDesc(hiveSchema.first, hiveTable.getBucketCols(), hiveTable.getNumBuckets(),
                hiveTable.getFileFormat(), location));
        table.setPartitionKeys(hiveSchema.second);

        // table.setViewOriginalText(hiveTable.getViewSql());
        // table.setViewExpandedText(hiveTable.getViewSql());
        table.setTableType("MANAGED_TABLE");
        table.setParameters(hiveTable.getProperties());
        return table;
    }

    private static StorageDescriptor toHiveStorageDesc(List<FieldSchema> columns,
            List<String> bucketCols, int numBuckets, String fileFormat, String location) {
        StorageDescriptor sd = new StorageDescriptor();
        sd.setCols(columns);
        setFileFormat(fileFormat, sd);
        if (StringUtils.isNotEmpty(location)) {
            sd.setLocation(location);
        }
        sd.setBucketCols(bucketCols);
        sd.setNumBuckets(numBuckets);
        Map<String, String> parameters = new HashMap<>();
        parameters.put("tag", "doris external hive talbe");
        sd.setParameters(parameters);
        return sd;
    }

    private static void setFileFormat(String fileFormat, StorageDescriptor sd) {
        String inputFormat;
        String outputFormat;
        String serDe;
        if (fileFormat.equalsIgnoreCase("orc")) {
            inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
            outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
            serDe = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";
        } else if (fileFormat.equalsIgnoreCase("parquet")) {
            inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
            outputFormat = "'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
            serDe = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";
        } else {
            throw new IllegalArgumentException("Creating table with an unsupported file format: " + fileFormat);
        }
        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib(serDe);
        sd.setSerdeInfo(serDeInfo);
        sd.setInputFormat(inputFormat);
        sd.setOutputFormat(outputFormat);
    }

    private static Pair<List<FieldSchema>, List<FieldSchema>> toHiveSchema(List<Column> columns,
            Set<String> partitionSet) {
        List<FieldSchema> hiveCols = new ArrayList<>();
        List<FieldSchema> hiveParts = new ArrayList<>();
        for (Column column : columns) {
            FieldSchema hiveFieldSchema = new FieldSchema();
            // TODO: add doc, just support doris type
            hiveFieldSchema.setType(HiveMetaStoreClientHelper.dorisTypeToHiveType(column.getType()));
            hiveFieldSchema.setName(column.getName());
            hiveFieldSchema.setComment(column.getComment());
            if (partitionSet.contains(column.getName())) {
                hiveParts.add(hiveFieldSchema);
            } else {
                hiveCols.add(hiveFieldSchema);
            }
        }
        return Pair.of(hiveCols, hiveParts);
    }
}
