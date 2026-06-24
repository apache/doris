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

package org.apache.doris.filesystem.cos;

import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CosFileSystemTest {

    @Test
    void globListWithLimit_usesProviderExactPathHeadControls() throws IOException {
        CosObjStorage mockStorage = Mockito.mock(CosObjStorage.class);
        Mockito.when(mockStorage.isUsePathStyle()).thenReturn(false);
        Mockito.when(mockStorage.listObjectsWithOptions(
                ArgumentMatchers.eq("cos://bucket/dir/file.csv"),
                ArgumentMatchers.<ObjectListOptions>any()))
                .thenReturn(new RemoteObjects(
                        List.of(new RemoteObject("dir/file.csv", "file.csv", null, 1L, 2L)),
                        false, null));
        Map<String, String> raw = new HashMap<>();
        raw.put("cos.endpoint", "cos.ap-guangzhou.myqcloud.com");
        raw.put("cos.region", "ap-guangzhou");
        raw.put("cos.access_key", "ak");
        raw.put("cos.secret_key", "sk");
        raw.put("s3_skip_list_for_deterministic_path", "false");
        raw.put("s3_head_request_max_paths", "100");
        CosFileSystem fs = new CosFileSystem(CosFileSystemProperties.of(raw), mockStorage);

        GlobListing listing = fs.globListWithLimit(
                Location.of("cos://bucket/dir/file.csv"), null, 0L, 0L);

        Assertions.assertEquals(1, listing.getFiles().size());
        Mockito.verify(mockStorage, Mockito.never()).headObject(ArgumentMatchers.anyString());
        Mockito.verify(mockStorage).listObjectsWithOptions(
                ArgumentMatchers.eq("cos://bucket/dir/file.csv"),
                ArgumentMatchers.<ObjectListOptions>any());
    }
}
