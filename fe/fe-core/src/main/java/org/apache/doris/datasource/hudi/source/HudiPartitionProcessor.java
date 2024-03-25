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

package org.apache.doris.datasource.hudi.source;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public abstract class HudiPartitionProcessor {

    public abstract void cleanUp();

    public abstract void cleanDatabasePartitions(String dbName);

    public abstract void cleanTablePartitions(String dbName, String tblName);

    public String[] getPartitionColumns(HoodieTableMetaClient tableMetaClient) {
        return tableMetaClient.getTableConfig().getPartitionFields().get();
    }

    public List<String> getAllPartitionNames(HoodieTableMetaClient tableMetaClient) throws IOException {
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(HoodieTableMetadataUtil.isFilesPartitionAvailable(tableMetaClient))
                .build();

        HoodieTableMetadata newTableMetadata = HoodieTableMetadata.create(
                new HoodieLocalEngineContext(tableMetaClient.getHadoopConf()), metadataConfig,
                tableMetaClient.getBasePathV2().toString(), true);

        return newTableMetadata.getAllPartitionPaths();
    }

    public List<String> getPartitionNamesBeforeOrEquals(HoodieTimeline timeline, String timestamp) {
        return new ArrayList<>(HoodieInputFormatUtils.getWritePartitionPaths(
                timeline.findInstantsBeforeOrEquals(timestamp).getInstants().stream().map(instant -> {
                    try {
                        return TimelineUtils.getCommitMetadata(instant, timeline);
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }).collect(Collectors.toList())));
    }

    public List<String> getPartitionNamesInRange(HoodieTimeline timeline, String startTimestamp, String endTimestamp) {
        return new ArrayList<>(HoodieInputFormatUtils.getWritePartitionPaths(
                timeline.findInstantsInRange(startTimestamp, endTimestamp).getInstants().stream().map(instant -> {
                    try {
                        return TimelineUtils.getCommitMetadata(instant, timeline);
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }).collect(Collectors.toList())));
    }

    public static List<String> parsePartitionValues(List<String> partitionColumns, String partitionPath) {
        if (partitionColumns.size() == 0) {
            // This is a non-partitioned table
            return Collections.emptyList();
        }
        String[] partitionFragments = partitionPath.split("/");
        if (partitionFragments.length != partitionColumns.size()) {
            if (partitionColumns.size() == 1) {
                // If the partition column size is not equal to the partition fragment size
                // and the partition column size is 1, we map the whole partition path
                // to the partition column which can benefit from the partition prune.
                String prefix = partitionColumns.get(0) + "=";
                String partitionValue;
                if (partitionPath.startsWith(prefix)) {
                    // support hive style partition path
                    partitionValue = partitionPath.substring(prefix.length());
                } else {
                    partitionValue = partitionPath;
                }
                // TODO: In hive, the specific characters like '=', '/' will be url encoded
                return Collections.singletonList(partitionValue);
            } else {
                // If the partition column size is not equal to the partition fragments size
                // and the partition column size > 1, we do not know how to map the partition
                // fragments to the partition columns and therefore return an empty tuple. We don't
                // fail outright so that in some cases we can fallback to reading the table as non-partitioned
                // one
                throw new RuntimeException("Failed to parse partition values of path: " + partitionPath);
            }
        } else {
            // If partitionSeqs.length == partitionSchema.fields.length
            // Append partition name to the partition value if the
            // HIVE_STYLE_PARTITIONING is disable.
            // e.g. convert "/xx/xx/2021/02" to "/xx/xx/year=2021/month=02"
            List<String> partitionValues = new ArrayList<>(partitionFragments.length);
            for (int i = 0; i < partitionFragments.length; i++) {
                String prefix = partitionColumns.get(i) + "=";
                if (partitionFragments[i].startsWith(prefix)) {
                    partitionValues.add(partitionFragments[i].substring(prefix.length()));
                } else {
                    partitionValues.add(partitionFragments[i]);
                }
            }
            return partitionValues;
        }
    }
}
