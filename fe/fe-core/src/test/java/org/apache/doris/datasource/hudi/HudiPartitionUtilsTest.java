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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class HudiPartitionUtilsTest {
    @Test
    public void testListPartitionsWithoutMetadataTable(@TempDir Path directory) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        HoodieTableMetaClient client = HoodieTableMetaClient.newTableBuilder()
                .setTableName("partition_dependency_test")
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setPartitionFields("date")
                .initTable(new HadoopStorageConfiguration(configuration), directory.toString());
        List<String> partitions = Arrays.asList("date=2026-11-03", "date=2026-11-04");
        for (String name : partitions) {
            Path partition = Files.createDirectories(directory.resolve(name));
            new HoodiePartitionMetadata(client.getStorage(), "20261104000000000", client.getBasePath(),
                    new StoragePath(partition.toUri()), Option.empty()).trySave();
        }
        Files.createDirectory(directory.resolve("unrelated-directory"));

        Assertions.assertEquals(new HashSet<>(partitions),
                new HashSet<>(HudiPartitionUtils.getAllPartitionNames(client)));
    }
}
