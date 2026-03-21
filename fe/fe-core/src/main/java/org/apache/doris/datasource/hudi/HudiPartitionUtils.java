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

package org.apache.doris.datasource.hudi;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Hudi partition utility helpers shared by scan/planner/meta cache components.
 */
public final class HudiPartitionUtils {
    private HudiPartitionUtils() {
    }

    public static List<String> getAllPartitionNames(HoodieTableMetaClient tableMetaClient) throws IOException {
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(HoodieTableMetadataUtil.isFilesPartitionAvailable(tableMetaClient))
                .build();
        HoodieTableMetadata newTableMetadata = HoodieTableMetadata.create(
                new HoodieLocalEngineContext(tableMetaClient.getStorageConf()), tableMetaClient.getStorage(),
                metadataConfig, tableMetaClient.getBasePath().toString(), true);
        return newTableMetadata.getAllPartitionPaths();
    }

    public static List<String> getPartitionNamesBeforeOrEquals(HoodieTimeline timeline, String timestamp) {
        return new ArrayList<>(HoodieTableMetadataUtil.getWritePartitionPaths(
                timeline.findInstantsBeforeOrEquals(timestamp).getInstants().stream().map(instant -> {
                    try {
                        return TimelineUtils.getCommitMetadata(instant, timeline);
                    } catch (IOException e) {
                        throw new RuntimeException(e.getMessage(), e);
                    }
                }).collect(Collectors.toList())));
    }

    public static List<String> parsePartitionValues(List<String> partitionColumns, String partitionPath) {
        if (partitionColumns.size() == 0) {
            // This is a non-partitioned table.
            return Collections.emptyList();
        }
        String[] partitionFragments = partitionPath.split("/");
        if (partitionFragments.length != partitionColumns.size()) {
            if (partitionColumns.size() == 1) {
                // If partition column size is 1, map whole partition path to this single partition column.
                String prefix = partitionColumns.get(0) + "=";
                String partitionValue = partitionPath.startsWith(prefix)
                        ? partitionPath.substring(prefix.length()) : partitionPath;
                return Collections.singletonList(FileUtils.unescapePathName(partitionValue));
            }
            throw new RuntimeException("Failed to parse partition values of path: " + partitionPath);
        }
        List<String> partitionValues = new ArrayList<>(partitionFragments.length);
        for (int i = 0; i < partitionFragments.length; i++) {
            String prefix = partitionColumns.get(i) + "=";
            if (partitionFragments[i].startsWith(prefix)) {
                partitionValues.add(FileUtils.unescapePathName(partitionFragments[i].substring(prefix.length())));
            } else {
                partitionValues.add(FileUtils.unescapePathName(partitionFragments[i]));
            }
        }
        return partitionValues;
    }
}
