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

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AzureUriTest {

    @Test
    void parseWasbScheme() throws IOException {
        AzureUri uri = AzureUri.parse("wasb://mycontainer@myaccount.blob.core.windows.net/path/to/file");
        assertEquals("wasb", uri.scheme());
        assertEquals("myaccount", uri.accountName());
        assertEquals("mycontainer", uri.container());
        assertEquals("path/to/file", uri.key());
    }

    @Test
    void parseWasbsScheme() throws IOException {
        AzureUri uri = AzureUri.parse("wasbs://container@account.blob.core.windows.net/key");
        assertEquals("wasbs", uri.scheme());
        assertEquals("account", uri.accountName());
        assertEquals("container", uri.container());
        assertEquals("key", uri.key());
    }

    @Test
    void parseAbfsScheme() throws IOException {
        AzureUri uri = AzureUri.parse("abfs://container@account.dfs.core.windows.net/dir/file");
        assertEquals("abfs", uri.scheme());
        assertEquals("account", uri.accountName());
        assertEquals("container", uri.container());
        assertEquals("dir/file", uri.key());
    }

    @Test
    void parseAbfssScheme() throws IOException {
        AzureUri uri = AzureUri.parse("abfss://container@account.dfs.core.windows.net/");
        assertEquals("abfss", uri.scheme());
        assertEquals("account", uri.accountName());
        assertEquals("container", uri.container());
        assertEquals("", uri.key());
    }

    @Test
    void parseHttpsScheme() throws IOException {
        AzureUri uri = AzureUri.parse("https://myaccount.blob.core.windows.net/mycontainer/path");
        assertEquals("https", uri.scheme());
        assertEquals("myaccount", uri.accountName());
        assertEquals("mycontainer", uri.container());
        assertEquals("path", uri.key());
    }

    @Test
    void parseS3CompatScheme() throws IOException {
        AzureUri uri = AzureUri.parse("s3://container/key/path");
        assertEquals("s3", uri.scheme());
        assertEquals("", uri.accountName());
        assertEquals("container", uri.container());
        assertEquals("key/path", uri.key());
    }

    @Test
    void parseWasbNoPath() throws IOException {
        AzureUri uri = AzureUri.parse("wasb://container@account.blob.core.windows.net");
        assertEquals("wasb", uri.scheme());
        assertEquals("account", uri.accountName());
        assertEquals("container", uri.container());
        assertEquals("", uri.key());
    }

    @Test
    void toStringReconstructsUri() throws IOException {
        AzureUri uri = AzureUri.parse("wasbs://mycontainer@myaccount.blob.core.windows.net/path/key");
        assertEquals("wasbs://mycontainer@myaccount/path/key", uri.toString());
    }

    @Test
    void nullPathThrows() {
        assertThrows(IOException.class, () -> AzureUri.parse(null));
    }

    @Test
    void emptyPathThrows() {
        assertThrows(IOException.class, () -> AzureUri.parse(""));
    }

    @Test
    void noSchemeThrows() {
        assertThrows(IOException.class, () -> AzureUri.parse("container/path"));
    }

    @Test
    void unsupportedSchemeThrows() {
        assertThrows(IOException.class, () -> AzureUri.parse("ftp://host/path"));
    }

    @Test
    void wasbMissingAtSignThrows() {
        assertThrows(IOException.class, () -> AzureUri.parse("wasb://container.blob.core.windows.net/path"));
    }
}
