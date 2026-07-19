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

package org.apache.doris.fs;

import org.apache.doris.datasource.property.storage.AbstractS3CompatibleProperties;
import org.apache.doris.datasource.property.storage.S3Properties;
import org.apache.doris.filesystem.FileSystem;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class FileSystemFactoryS3ExpressScopeTest {

    @After
    public void tearDown() {
        FileSystemFactory.clearProviderCache();
    }

    @Test
    public void testRawMapCannotInjectS3ExpressImportMarker() throws Exception {
        FileSystemPluginManager manager = Mockito.mock(FileSystemPluginManager.class);
        Mockito.when(manager.createFileSystem(Mockito.anyMap())).thenReturn(Mockito.mock(FileSystem.class));
        FileSystemFactory.initPluginManager(manager);
        Map<String, String> rawProperties = new HashMap<>();
        rawProperties.put(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ, "true");

        FileSystemFactory.getFileSystem(rawProperties);

        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(manager).createFileSystem(captor.capture());
        Assert.assertFalse(captor.getValue()
                .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
        Assert.assertTrue(rawProperties
                .containsKey(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
    }

    @Test
    public void testTypedS3ExpressImportKeepsTrustedMarker() throws Exception {
        FileSystemPluginManager manager = Mockito.mock(FileSystemPluginManager.class);
        Mockito.when(manager.createFileSystem(Mockito.anyMap())).thenReturn(Mockito.mock(FileSystem.class));
        FileSystemFactory.initPluginManager(manager);
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "s3://analytics--usw2-az1--x-s3/data.parquet");
        properties.put("s3.provider", "AWS");
        properties.put("s3.access_key", "ak");
        properties.put("s3.secret_key", "sk");

        FileSystemFactory.getFileSystem(S3Properties.createForS3ExpressImport(properties));

        ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(manager).createFileSystem(captor.capture());
        Assert.assertEquals("true", captor.getValue()
                .get(AbstractS3CompatibleProperties.S3_EXPRESS_IMPORT_READ));
    }
}
