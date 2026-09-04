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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.io.FileIO;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IcebergAzureFileIoClasspathTest {

    @Test
    void adlsFileIoAndAzureDataLakeSdkAreAvailableToTheConnector() throws Exception {
        Class<?> adlsFileIo = Class.forName("org.apache.iceberg.azure.adlsv2.ADLSFileIO");
        Object instance = adlsFileIo.getConstructor().newInstance();

        Assertions.assertInstanceOf(FileIO.class, instance);
        Assertions.assertNotNull(
                Class.forName("com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder"));
        Assertions.assertNotNull(
                Class.forName("com.azure.core.http.jdk.httpclient.JdkHttpClientProvider"));
        Assertions.assertThrows(ClassNotFoundException.class,
                () -> Class.forName("com.azure.core.http.netty.NettyAsyncHttpClientProvider"));
    }
}
