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

package org.apache.doris.common.util;

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.FileSystemType;
import org.apache.doris.thrift.TFileType;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LocationPathTest {

    private static Map<StorageProperties.Type, StorageProperties> STORAGE_PROPERTIES_MAP = new HashMap<>();

    static {
        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "namenode:8020");
        props.put("s3.endpoint", "s3.us-east-2.amazonaws.com");
        props.put("s3.access_key", "access_key");
        props.put("s3.secret_key", "secret_key");
        props.put("oss.endpoint", "oss-cn-beijing.aliyuncs.com");
        props.put("oss.access_key", "access_key");
        props.put("oss.secret_key", "secret_key");
        props.put("cos.endpoint", "cos.ap-guangzhou.myqcloud.com");
        props.put("cos.access_key", "access_key");
        props.put("cos.secret_key", "secret_key");
        props.put("obs.endpoint", "obs.cn-north-4.myhuaweicloud.com");
        props.put("obs.access_key", "access_key");
        props.put("obs.secret_key", "secret_key");
        props.put("fs.defaultFS", "hdfs://namenode:8020");
        props.put("azure.endpoint", "https://mystorageaccount.blob.core.windows.net");
        props.put("azure.access_key", "access_key");
        props.put("azure.secret_key", "secret_key");
        props.put("broker.name", "mybroker");

        try {
            STORAGE_PROPERTIES_MAP = StorageProperties.createAll(props).stream()
                    .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
        } catch (UserException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testHdfsLocationConvert() throws UserException {
        // non HA
        LocationPath locationPath = LocationPath.of("hdfs://dir/file.path");
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("hdfs://"));
        Assertions.assertEquals(locationPath.getFsIdentifier(), "hdfs://dir");

        // HA props
        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns");
        //HdfsProperties hdfsProperties = (HdfsProperties) StorageProperties.createPrimary( props);
        Map<StorageProperties.Type, StorageProperties> storagePropertiesMap = StorageProperties.createAll(props).stream()
                .collect(java.util.stream.Collectors.toMap(StorageProperties::getType, Function.identity()));
        locationPath = LocationPath.of("hdfs:///dir/file.path", storagePropertiesMap);
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("hdfs://")
                && !locationPath.getNormalizedLocation().startsWith("hdfs:///"));

        // nonstandard '/' for hdfs path
        locationPath = LocationPath.of("hdfs:/dir/file.path", storagePropertiesMap);
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("hdfs://"));
    }

    @Test
    public void testJFSLocationConvert() {
        LocationPath locationPath = LocationPath.of("jfs://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("jfs://"));
        // BE
        String loc = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(loc.startsWith("jfs://"));
        Assertions.assertEquals(FileSystemType.JFS, locationPath.getFileSystemType());
        Assertions.assertEquals("jfs://test.com", locationPath.getFsIdentifier());
        Assertions.assertEquals(TFileType.FILE_BROKER, locationPath.getTFileTypeForBE());
    }

    @Disabled("not support in master")
    @Test
    public void testGSLocationConvert() {
        /*        Map<String, String> rangeProps = new HashMap<>();

        // use s3 client to access gs
        LocationPath locationPath = LocationPath.of("gs://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("s3://"));
        // BE
        String beLoc = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLoc.startsWith("s3://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLoc, Collections.emptyMap(), null).first,
                FileSystemType.S3);*/
    }

    @Test
    public void testOSSLocationConvert() {
        LocationPath locationPath = LocationPath.of("oss://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("oss://"));
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("oss://"));
        Assertions.assertEquals(FileSystemType.S3, locationPath.getFileSystemType());
        Assertions.assertEquals(TFileType.FILE_S3, locationPath.getTFileTypeForBE());

        // test oss-hdfs
        /* rangeProps.put(OssProperties.ENDPOINT, "oss-dls.aliyuncs.com");
        locationPath = LocationPath.of("oss://test.oss-dls.aliyuncs.com/path", rangeProps);
        Assertions.assertEquals("oss://test.oss-dls.aliyuncs.com/path", locationPath.getNormalizedLocation());
        Assertions.assertEquals(LocationPath.getFSIdentity(locationPath.getNormalizedLocation(), rangeProps, null).first,
                FileSystemType.HDFS);
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("oss://test.oss-dls.aliyuncs"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("oss://test.oss-dls.aliyuncs"));
        Assertions.assertEquals(locationPath.getFileSystemType(), FileSystemType.HDFS);*/
    }

    @Test
    public void testCOSLocationConvert() {
        LocationPath locationPath = LocationPath.of("cos://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("cos://"));
        String beLocation = locationPath.toStorageLocation().toString();
        // BE
        Assertions.assertTrue(beLocation.startsWith("cos://"));
        Assertions.assertEquals(locationPath.getFileSystemType(), FileSystemType.S3);

        locationPath = LocationPath.of("cosn://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("cosn://"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("cosn://"));
        Assertions.assertEquals(FileSystemType.S3, locationPath.getFileSystemType());
        Assertions.assertEquals(TFileType.FILE_S3, locationPath.getTFileTypeForBE());

        locationPath = LocationPath.of("ofs://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("ofs://"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("ofs://"));
        Assertions.assertEquals(FileSystemType.OFS, locationPath.getFileSystemType());
        Assertions.assertEquals(TFileType.FILE_BROKER, locationPath.getTFileTypeForBE());

        // GFS is now equals to DFS
        locationPath = LocationPath.of("gfs://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("gfs://"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("gfs://"));
        Assertions.assertEquals(locationPath.getFileSystemType(),
                FileSystemType.HDFS);
        Assertions.assertEquals(TFileType.FILE_BROKER, locationPath.getTFileTypeForBE());
    }

    @Test
    public void testOBSLocationConvert() {
        LocationPath locationPath = LocationPath.of("obs://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("obs://"));
        Assertions.assertEquals(locationPath.getFileSystemType(),
                FileSystemType.S3);
        Assertions.assertEquals(locationPath.getTFileTypeForBE(),
                TFileType.FILE_S3);
    }

    @Test
    public void testUnsupportedLocationConvert() {
        // when use unknown location, pass to BE
        LocationPath locationPath = LocationPath.of("unknown://test.com");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().startsWith("unknown://"));
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("unknown://"));
    }

    @Test
    public void testNoSchemeLocation() {
        // when use unknown location, pass to BE
        LocationPath locationPath = LocationPath.of("/path/to/local");
        // FE
        Assertions.assertTrue(locationPath.getNormalizedLocation().equalsIgnoreCase("/path/to/local"));
        Assertions.assertTrue(StringUtils.isEmpty(locationPath.getSchema()));
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.equalsIgnoreCase("/path/to/local"));
    }

    @Test
    public void testLocationProperties() {
        assertNormalize("hdfs://namenode:8020/path/to/file", "hdfs://namenode:8020/path/to/file");
        assertNormalize("hdfs://namenode/path/to/file", "hdfs://namenode/path/to/file");
        assertNormalize("s3://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("s3a://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("s3n://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("oss://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("cos://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("obs://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("ofs://bucket/path/to/file", "ofs://bucket/path/to/file");
        assertNormalize("jfs://bucket/path/to/file", "jfs://bucket/path/to/file");
        assertNormalize("gfs://bucket/path/to/file", "gfs://bucket/path/to/file");
        assertNormalize("cosn://bucket/path/to/file", "s3://bucket/path/to/file");
        assertNormalize("viewfs://cluster/path/to/file", "viewfs://cluster/path/to/file");
        assertNormalize("/path/to/file", "hdfs://namenode:8020/path/to/file");
        assertNormalize("hdfs:///path/to/file", "hdfs://namenode:8020/path/to/file");
    }

    private void assertNormalize(String input, String expected) {
        LocationPath locationPath = LocationPath.of(input, STORAGE_PROPERTIES_MAP);
        String actual = locationPath.getNormalizedLocation();
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void testMinIoProperties() throws UserException {
        Map<String, String> props = new HashMap<>();
        props.put("minio.endpoint", "https://minio.example.com");
        props.put("minio.access_key", "access_key");
        props.put("minio.secret_key", "secret_key");

        StorageProperties minioProperties = StorageProperties.createAll(props).stream()
                .filter(p -> p.getType() == StorageProperties.Type.MINIO)
                .findFirst()
                .orElseThrow(() -> new UserException("MinIO properties not found"));
        Map<StorageProperties.Type, StorageProperties> storagePropertiesMap = new HashMap<>();
        storagePropertiesMap.put(StorageProperties.Type.MINIO, minioProperties);
        LocationPath locationPath = LocationPath.of("s3a://minio.example.com/bucket/path", storagePropertiesMap);
        Assertions.assertEquals("s3://minio.example.com/bucket/path", locationPath.getNormalizedLocation());
        Assertions.assertEquals(FileSystemType.S3, locationPath.getFileSystemType());
        Assertions.assertEquals(TFileType.FILE_S3, locationPath.getTFileTypeForBE());
    }

    @Test
    public void testHdfsStorageLocationConvert() {
        String location = "hdfs://172.16.0.35:8020/user/hive/warehouse/partition_special_characters_1/pt=1,1%3D1, 3%3D2+1, 1%3D3-2, 3%2F3%3D1, 2%2F2%3D1, 2%2F1%3D2, 2%2F1%3D2 +1 -1,2%2F1%3D2 %2A3 %2F3";
        LocationPath locationPath = LocationPath.of(location, STORAGE_PROPERTIES_MAP);
        Assertions.assertEquals(FileSystemType.HDFS, locationPath.getFileSystemType());
        Assertions.assertEquals(location, locationPath.getNormalizedLocation());
        locationPath = LocationPath.of(location);
        Assertions.assertEquals(location, locationPath.getNormalizedLocation());
    }

}
