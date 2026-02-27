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

import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LocationPath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class OzonePropertiesTest {
    private Map<String, String> origProps;

    @BeforeEach
    public void setup() {
        origProps = new HashMap<>();
    }

    @Test
    public void testValidOzoneConfiguration() {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("ozone.endpoint", "http://ozone-s3g:9878");
        origProps.put("ozone.access_key", "hadoop");
        origProps.put("ozone.secret_key", "hadoop");

        OzoneProperties ozoneProperties = (OzoneProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProps = ozoneProperties.getBackendConfigProperties();

        Assertions.assertEquals(StorageProperties.Type.OZONE, ozoneProperties.getType());
        Assertions.assertEquals("http://ozone-s3g:9878", ozoneProperties.getEndpoint());
        Assertions.assertEquals("hadoop", ozoneProperties.getAccessKey());
        Assertions.assertEquals("hadoop", ozoneProperties.getSecretKey());
        Assertions.assertEquals("us-east-1", ozoneProperties.getRegion());
        Assertions.assertEquals("true", ozoneProperties.getUsePathStyle());

        Assertions.assertEquals("http://ozone-s3g:9878", backendProps.get("AWS_ENDPOINT"));
        Assertions.assertEquals("hadoop", backendProps.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("hadoop", backendProps.get("AWS_SECRET_KEY"));
        Assertions.assertEquals("us-east-1", backendProps.get("AWS_REGION"));
        Assertions.assertEquals("true", backendProps.get("use_path_style"));
    }

    @Test
    public void testS3PropertiesBinding() {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("s3.endpoint", "http://ozone-s3g:9878");
        origProps.put("s3.access_key", "hadoop");
        origProps.put("s3.secret_key", "hadoop");
        origProps.put("use_path_style", "true");
        origProps.put("s3.region", "us-east-1");

        OzoneProperties ozoneProperties = (OzoneProperties) StorageProperties.createPrimary(origProps);
        Map<String, String> backendProps = ozoneProperties.getBackendConfigProperties();

        Assertions.assertEquals("http://ozone-s3g:9878", ozoneProperties.getEndpoint());
        Assertions.assertEquals("hadoop", ozoneProperties.getAccessKey());
        Assertions.assertEquals("hadoop", ozoneProperties.getSecretKey());
        Assertions.assertEquals("true", ozoneProperties.getUsePathStyle());

        Assertions.assertEquals("http://ozone-s3g:9878", backendProps.get("AWS_ENDPOINT"));
        Assertions.assertEquals("hadoop", backendProps.get("AWS_ACCESS_KEY"));
        Assertions.assertEquals("hadoop", backendProps.get("AWS_SECRET_KEY"));
    }

    @Test
    public void testFsS3aPropertiesAreNotSupported() {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("fs.s3a.endpoint", "http://ozone-s3g:9878");
        origProps.put("fs.s3a.access.key", "hadoop");
        origProps.put("fs.s3a.secret.key", "hadoop");

        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Property ozone.endpoint is required.",
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testCreateAllWithDefaultFs() throws UserException {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("fs.defaultFS", "s3a://dn-data/");
        origProps.put("s3.endpoint", "http://ozone-s3g:9878");
        origProps.put("s3.access_key", "hadoop");
        origProps.put("s3.secret_key", "hadoop");
        origProps.put("use_path_style", "true");

        List<StorageProperties> properties = StorageProperties.createAll(origProps);
        Assertions.assertEquals(HdfsProperties.class, properties.get(0).getClass());
        Assertions.assertEquals(OzoneProperties.class, properties.get(1).getClass());

        Map<StorageProperties.Type, StorageProperties> propertiesMap = properties.stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
        LocationPath locationPath = LocationPath.of("s3a://dn-data/warehouse/test_table", propertiesMap);
        Assertions.assertTrue(locationPath.getStorageProperties() instanceof OzoneProperties);
    }

    @Test
    public void testCreateAllWithDefaultFsAndOzoneProperties() throws UserException {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("fs.defaultFS", "s3a://dn-data/");
        origProps.put("ozone.endpoint", "http://ozone-s3g:9878");
        origProps.put("ozone.access_key", "hadoop");
        origProps.put("ozone.secret_key", "hadoop");
        origProps.put("ozone.use_path_style", "true");
        origProps.put("ozone.region", "us-east-1");

        List<StorageProperties> properties = StorageProperties.createAll(origProps);
        Assertions.assertEquals(HdfsProperties.class, properties.get(0).getClass());
        Assertions.assertEquals(OzoneProperties.class, properties.get(1).getClass());

        OzoneProperties ozoneProperties = (OzoneProperties) properties.get(1);
        Assertions.assertEquals("hadoop", ozoneProperties.getHadoopStorageConfig().get("fs.s3a.access.key"));
        Assertions.assertEquals("hadoop", ozoneProperties.getHadoopStorageConfig().get("fs.s3a.secret.key"));
        Assertions.assertEquals("http://ozone-s3g:9878", ozoneProperties.getHadoopStorageConfig().get("fs.s3a.endpoint"));
        Assertions.assertEquals("us-east-1", ozoneProperties.getHadoopStorageConfig().get("fs.s3a.endpoint.region"));
        Assertions.assertEquals("true", ozoneProperties.getHadoopStorageConfig().get("fs.s3a.path.style.access"));
    }

    @Test
    public void testMissingAccessKeyOrSecretKey() {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("ozone.endpoint", "http://ozone-s3g:9878");
        origProps.put("ozone.access_key", "hadoop");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Both the access key and the secret key must be set.",
                () -> StorageProperties.createPrimary(origProps));

        origProps.remove("ozone.access_key");
        origProps.put("ozone.secret_key", "hadoop");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Both the access key and the secret key must be set.",
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testMissingEndpoint() {
        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        origProps.put("ozone.access_key", "hadoop");
        origProps.put("ozone.secret_key", "hadoop");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Property ozone.endpoint is required.",
                () -> StorageProperties.createPrimary(origProps));
    }

    @Test
    public void testRequireExplicitFsOzoneSupport() throws UserException {
        origProps.put("ozone.endpoint", "http://127.0.0.1:9878");
        origProps.put("ozone.access_key", "hadoop");
        origProps.put("ozone.secret_key", "hadoop");

        List<StorageProperties> propertiesWithoutFlag = StorageProperties.createAll(origProps);
        Assertions.assertEquals(1, propertiesWithoutFlag.size());
        Assertions.assertEquals(HdfsProperties.class, propertiesWithoutFlag.get(0).getClass());

        origProps.put(StorageProperties.FS_OZONE_SUPPORT, "true");
        List<StorageProperties> propertiesWithFlag = StorageProperties.createAll(origProps);
        Assertions.assertEquals(2, propertiesWithFlag.size());
        Assertions.assertEquals(HdfsProperties.class, propertiesWithFlag.get(0).getClass());
        Assertions.assertEquals(OzoneProperties.class, propertiesWithFlag.get(1).getClass());
    }
}
