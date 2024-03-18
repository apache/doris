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

import org.apache.doris.common.UserException;
import org.apache.doris.fs.remote.BrokerFileSystem;
import org.apache.doris.fs.remote.RemoteFileSystem;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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
}
