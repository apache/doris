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

import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.List;

class AzureAdlsLocationContractTest {

    @ParameterizedTest
    @CsvSource({"abfs,dfs", "abfss,dfs", "wasb,blob", "wasbs,blob"})
    void icebergAdlsLocationAndSdkPreservePercentSequences(String scheme, String service)
            throws ReflectiveOperationException, IOException {
        // ADLSLocation is package-private. Keep this dependency and reflective
        // access in tests, without adding Iceberg types to the provider API.
        Class<?> locationClass = Class.forName("org.apache.iceberg.azure.adlsv2.ADLSLocation");
        Constructor<?> constructor = locationClass.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        Method path = locationClass.getDeclaredMethod("path");
        path.setAccessible(true);

        HttpClient httpClient = Mockito.mock(HttpClient.class);
        BlobContainerClient container = new BlobContainerClientBuilder()
                .endpoint("https://account.blob.core.windows.net/container")
                .httpClient(httpClient)
                .buildClient();
        for (String key : List.of("data/p=a%2Fb/file.parquet", "data/p=a%20b/file.parquet",
                "data/p=a%252Fb/file.parquet", "data/p=a+b/http://example//file",
                "data/p=100%/file.parquet", "data/p=%2/%GG/file.parquet")) {
            String location = scheme + "://container@account." + service + ".core.windows.net/" + key;
            String icebergKey = (String) path.invoke(constructor.newInstance(location));
            AzureUri nativeLocation = AzureUri.parse(location);

            Assertions.assertEquals(key, icebergKey, location);
            Assertions.assertEquals(icebergKey, nativeLocation.key(), location);
            Assertions.assertEquals(location, nativeLocation.toString());
            // This is the Blob SDK object-name API also used by DataLakeFileClient.
            // Decode its URL once to check the name that reaches Azure, without I/O.
            BlobClient sdkClient = container.getBlobClient(icebergKey);
            Assertions.assertEquals(key, sdkClient.getBlobName());
            Assertions.assertEquals("/container/" + key, URI.create(sdkClient.getBlobUrl()).getPath());
        }
        Mockito.verifyNoInteractions(httpClient);
    }
}
