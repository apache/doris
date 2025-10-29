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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.datasource.property.storage.exception.StoragePropertiesException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AzurePropertyUtilsTest {

    // ---------- validateAndNormalizeUri Tests ----------

    @Test
    public void testWasbsUri() throws Exception {
        String input = "wasbs://container@account.blob.core.windows.net/data/file.txt";
        String expected = "s3://container/data/file.txt";
        Assertions.assertEquals(expected, AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testS3Uri() throws Exception {
        String input = "s3://container/data/file.txt";
        String expected = "s3://container/data/file.txt";
        Assertions.assertEquals(expected, AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testAbfssUriWithoutPath() throws Exception {
        String input = "abfss://container@account.blob.core.windows.net";
        String expected = "s3://container";
        Assertions.assertEquals(expected, AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testHttpUri() throws Exception {
        String input = "http://account.blob.core.windows.net/container/folder/file.csv";
        String expected = "s3://container/folder/file.csv";
        Assertions.assertEquals(expected, AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testHttpsUriWithoutPath() throws Exception {
        String input = "https://account.blob.core.windows.net/container";
        String expected = "s3://container";
        Assertions.assertEquals(expected, AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testHttpsUriWithPath() throws Exception {
        String input = "https://account.blob.core.windows.net/container/data/file.parquet";
        String expected = "s3://container/data/file.parquet";
        Assertions.assertEquals(expected, AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testInvalidAzureScheme() {
        String input = "ftp://container@account.blob.core.windows.net/data/file.txt";
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testMissingAtInWasbUri() {
        String input = "wasb://container/account.blob.core.windows.net/data";
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testHttpsUriMissingHost() {
        String input = "https:///container/file.txt"; // missing host
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testHttpsUriNotAzureBlob() {
        String input = "https://account.otherdomain.com/container/file.txt";
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndNormalizeUri(input));
    }

    @Test
    public void testBlankUri() {
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndNormalizeUri(" "));
    }

    // ---------- validateAndGetUri Tests ----------

    @Test
    public void testValidateAndGetUriNormal() {
        Map<String, String> props = new HashMap<>();
        props.put("uri", "wasbs://container@account.blob.core.windows.net/data/file.txt");
        Assertions.assertEquals("wasbs://container@account.blob.core.windows.net/data/file.txt",
                AzurePropertyUtils.validateAndGetUri(props));
    }

    @Test
    public void testValidateAndGetUriCaseInsensitive() {
        Map<String, String> props = new HashMap<>();
        props.put("URI", "wasbs://container@account.blob.core.windows.net/data/file.txt");
        Assertions.assertEquals("wasbs://container@account.blob.core.windows.net/data/file.txt",
                AzurePropertyUtils.validateAndGetUri(props));
    }

    @Test
    public void testValidateAndGetUriMissing() {
        Map<String, String> props = new HashMap<>();
        props.put("path", "value");
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndGetUri(props));
    }

    @Test
    public void testValidateAndGetUriEmptyMap() {
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndGetUri(new HashMap<>()));
    }

    @Test
    public void testValidateAndGetUriNullMap() {
        Assertions.assertThrows(StoragePropertiesException.class, () ->
                AzurePropertyUtils.validateAndGetUri(null));
    }
}
