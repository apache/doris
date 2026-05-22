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

package org.apache.doris.filesystem.azure;

import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.spi.FileSystemProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * SPI provider for Azure Blob Storage.
 *
 * <p>Registered via META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider.
 *
 * <p>Identified by the presence of {@code AZURE_ACCOUNT_NAME}, {@code azure.account_name},
 * or an endpoint that contains a known Azure Blob Storage host suffix from one of the
 * sovereign clouds.
 */
public class AzureFileSystemProvider implements FileSystemProvider {

    /**
     * Recognised Azure Blob Storage host suffixes across sovereign clouds.
     * Includes Azure Public, Azure China, Azure US Government, and the deprecated
     * Azure Germany cloud (still spec'd for completeness).
     */
    private static final List<String> AZURE_BLOB_HOST_SUFFIXES = Arrays.asList(
            "blob.core.windows.net",
            "blob.core.chinacloudapi.cn",
            "blob.core.usgovcloudapi.net",
            "blob.core.cloudapi.de");

    @Override
    public boolean supports(Map<String, String> properties) {
        if (properties.containsKey(AzureObjStorage.PROP_ACCOUNT_NAME)) {
            return true;
        }
        if (properties.containsKey(AzureObjStorage.PROP_ACCOUNT_NAME_ALT)) {
            return true;
        }
        String endpoint = properties.get(AzureObjStorage.PROP_ENDPOINT);
        if (endpoint == null) {
            endpoint = properties.get(AzureObjStorage.PROP_ENDPOINT_ALT);
        }
        if (endpoint == null) {
            return false;
        }
        for (String suffix : AZURE_BLOB_HOST_SUFFIXES) {
            if (endpoint.contains(suffix)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public FileSystem create(Map<String, String> properties) throws IOException {
        return new AzureFileSystem(new AzureObjStorage(properties));
    }

    @Override
    public String name() {
        return "AZURE";
    }
}
