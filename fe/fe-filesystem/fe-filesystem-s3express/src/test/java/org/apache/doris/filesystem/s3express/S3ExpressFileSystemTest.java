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

package org.apache.doris.filesystem.s3express;

import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.filesystem.s3.S3ObjStorage;
import org.apache.doris.filesystem.spi.ObjectListOptions;
import org.apache.doris.filesystem.spi.RemoteObject;
import org.apache.doris.filesystem.spi.RemoteObjects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class S3ExpressFileSystemTest {

    @Test
    void globListWithLimit_fallsBackToSlashTerminatedStaticPrefix() throws IOException {
        S3ExpressFileSystemProperties properties = S3ExpressFileSystemProperties.of(Map.of(
                "s3.endpoint", "https://s3express-usw2-az1.us-west-2.amazonaws.com",
                "s3.region", "us-west-2"));
        S3ObjStorage mockStorage = Mockito.mock(S3ObjStorage.class);
        Mockito.when(mockStorage.getSupportedSchemes()).thenReturn(properties.getSupportedSchemes());
        Mockito.when(mockStorage.listObjectsWithOptions(
                        ArgumentMatchers.anyString(), ArgumentMatchers.<ObjectListOptions>any()))
                .thenReturn(new RemoteObjects(List.of(), false, null));
        Mockito.when(mockStorage.listObjectsWithOptions(
                        ArgumentMatchers.eq("s3://bucket/data/"),
                        ArgumentMatchers.<ObjectListOptions>any()))
                .thenReturn(new RemoteObjects(
                        List.of(
                                new RemoteObject("data/a.csv", "a.csv", null, 10L, 0L),
                                new RemoteObject("data/b.csv", "b.csv", null, 20L, 0L)),
                        false, null));
        S3ExpressFileSystem fileSystem = new S3ExpressFileSystem(properties, mockStorage);

        GlobListing listing = fileSystem.globListWithLimit(
                Location.of("s3://bucket/data/[ab]*.csv"), null, 0L, 0L);

        Assertions.assertEquals(2, listing.getFiles().size());
        Assertions.assertEquals("data/", listing.getPrefix());
        Mockito.verify(mockStorage).listObjectsWithOptions(
                ArgumentMatchers.eq("s3://bucket/data/"),
                ArgumentMatchers.<ObjectListOptions>any());
        Mockito.verify(mockStorage, Mockito.never()).listObjectsWithOptions(
                ArgumentMatchers.eq("s3://bucket/data/a"),
                ArgumentMatchers.<ObjectListOptions>any());
        Mockito.verify(mockStorage, Mockito.never()).listObjectsWithOptions(
                ArgumentMatchers.eq("s3://bucket/data/b"),
                ArgumentMatchers.<ObjectListOptions>any());
    }
}
