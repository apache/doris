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

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.util.LocationPath.Scheme;
import org.apache.doris.datasource.property.constants.OssProperties;
import org.apache.doris.fs.FileSystemType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LocationPathTest {

    @Test
    public void testHdfsLocationConvert() {
        // non HA
        Map<String, String> rangeProps = new HashMap<>();
        LocationPath locationPath = new LocationPath("hdfs://dir/file.path", rangeProps);
        Assertions.assertTrue(locationPath.get().startsWith("hdfs://"));

        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("hdfs://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.DFS);

        // HA props
        Map<String, String> props = new HashMap<>();
        props.put("dfs.nameservices", "ns");
        locationPath = new LocationPath("hdfs:///dir/file.path", props);
        Assertions.assertTrue(locationPath.get().startsWith("hdfs://")
                && !locationPath.get().startsWith("hdfs:///"));

        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("hdfs://") && !beLocation.startsWith("hdfs:///"));

        // nonstandard '/' for hdfs path
        locationPath = new LocationPath("hdfs:/dir/file.path", props);
        Assertions.assertTrue(locationPath.get().startsWith("hdfs://"));

        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("hdfs://"));

        // empty ha nameservices
        props.put("dfs.nameservices", "");
        locationPath = new LocationPath("hdfs:/dir/file.path", props);

        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(locationPath.get().startsWith("/dir")
                && !locationPath.get().startsWith("hdfs://"));
        Assertions.assertTrue(beLocation.startsWith("/dir") && !beLocation.startsWith("hdfs://"));

        props.clear();
        props.put(HdfsResource.HADOOP_FS_NAME, "hdfs://test.com");
        locationPath = new LocationPath("/dir/file.path", props);
        Assertions.assertTrue(locationPath.get().startsWith("hdfs://"));
        Assertions.assertEquals("hdfs://test.com/dir/file.path", locationPath.get());
        Assertions.assertEquals("hdfs://test.com/dir/file.path", locationPath.toStorageLocation().toString());
        props.clear();
        props.put(HdfsResource.HADOOP_FS_NAME, "oss://test.com");
        locationPath = new LocationPath("/dir/file.path", props);
        Assertions.assertTrue(locationPath.get().startsWith("oss://"));
        Assertions.assertEquals("oss://test.com/dir/file.path", locationPath.get());
        Assertions.assertEquals("s3://test.com/dir/file.path", locationPath.toStorageLocation().toString());
    }

    @Test
    public void testJFSLocationConvert() {
        String loc;
        Map<String, String> rangeProps = new HashMap<>();

        LocationPath locationPath = new LocationPath("jfs://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("jfs://"));
        // BE
        loc = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(loc.startsWith("jfs://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(loc, null).first, FileSystemType.JFS);
    }

    @Test
    public void testGSLocationConvert() {
        Map<String, String> rangeProps = new HashMap<>();

        // use s3 client to access gs
        LocationPath locationPath = new LocationPath("gs://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("s3://"));
        // BE
        String beLoc = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLoc.startsWith("s3://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLoc, null).first, FileSystemType.S3);
    }

    @Test
    public void testOSSLocationConvert() {
        Map<String, String> rangeProps = new HashMap<>();
        LocationPath locationPath = new LocationPath("oss://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("oss://"));
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("s3://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.S3);

        rangeProps.put(OssProperties.ENDPOINT, "oss-dls.aliyuncs.com");
        locationPath = new LocationPath("oss://test.oss-dls.aliyuncs.com/path", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("oss://test.oss-dls.aliyuncs"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("oss://test.oss-dls.aliyuncs"));
        Assertions.assertEquals(locationPath.getFileSystemType(), FileSystemType.DFS);

    }

    @Test
    public void testCOSLocationConvert() {
        Map<String, String> rangeProps = new HashMap<>();
        LocationPath locationPath = new LocationPath("cos://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("cos://"));
        String beLocation = locationPath.toStorageLocation().toString();
        // BE
        Assertions.assertTrue(beLocation.startsWith("s3://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.S3);

        locationPath = new LocationPath("cosn://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("cosn://"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("s3://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.S3);

        locationPath = new LocationPath("ofs://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("ofs://"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("ofs://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.OFS);

        // GFS is now equals to DFS
        locationPath = new LocationPath("gfs://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("gfs://"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("gfs://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.DFS);
    }

    @Test
    public void testOBSLocationConvert() {
        Map<String, String> rangeProps = new HashMap<>();
        LocationPath locationPath = new LocationPath("obs://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("obs://"));
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("s3://"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.S3);
    }

    @Test
    public void testUnsupportedLocationConvert() {
        // when use unknown location, pass to BE
        Map<String, String> rangeProps = new HashMap<>();
        LocationPath locationPath = new LocationPath("unknown://test.com", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("unknown://"));
        Assertions.assertTrue(locationPath.getScheme() == Scheme.UNKNOWN);
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("unknown://"));
    }

    @Test
    public void testNoSchemeLocation() {
        // when use unknown location, pass to BE
        Map<String, String> rangeProps = new HashMap<>();
        LocationPath locationPath = new LocationPath("/path/to/local", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().equalsIgnoreCase("/path/to/local"));
        Assertions.assertTrue(locationPath.getScheme() == Scheme.NOSCHEME);
        // BE
        String beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.equalsIgnoreCase("/path/to/local"));
    }

    @Test
    public void testLocalFileSystem() {
        HashMap<String, String> props = new HashMap<>();
        props.put("fs.defaultFS", "hdfs:///xyz");
        LocationPath p1 = new LocationPath("file:///abc/def", props);
        Assertions.assertEquals(Scheme.LOCAL, p1.getScheme());
        LocationPath p2 = new LocationPath("file:/abc/def", props);
        Assertions.assertEquals(Scheme.LOCAL, p2.getScheme());
        LocationPath p3 = new LocationPath("file://authority/abc/def", props);
        Assertions.assertEquals(Scheme.LOCAL, p3.getScheme());
    }

    @Test
    public void testNormalizedHdfsPath() {
        // Test case 1: Path with special characters that need encoding
        // Input: Path with spaces and special characters
        // Expected: Characters are properly encoded while preserving / and :
        String location = "hdfs://namenode/path with spaces/<special>chars";
        String host = "";
        boolean enableOssRootPolicy = false;
        String result = LocationPath.normalizedHdfsPath(location, host, enableOssRootPolicy);
        Assertions.assertEquals("hdfs://namenode/path with spaces/<special>chars", result);

        // Test case 2: Empty host in URI with host parameter provided
        // Input: hdfs:///, host = nameservice
        // Expected: hdfs://nameservice/
        location = "hdfs:///path/to/file";
        host = "nameservice";
        result = LocationPath.normalizedHdfsPath(location, host, false);
        Assertions.assertEquals("hdfs://nameservice//path/to/file", result);

        // Test case 3: Broken prefix case (hdfs:/ instead of hdfs://)
        // Input: hdfs:/path, host = nameservice
        // Expected: hdfs://nameservice/path
        location = "hdfs:/path/to/file";
        host = "nameservice";
        result = LocationPath.normalizedHdfsPath(location, host, false);
        Assertions.assertEquals("hdfs://nameservice/path/to/file", result);

        // Test case 4: Empty host parameter with enableOssRootPolicy=true
        // Input: hdfs://customized_host/path
        // Expected: hdfs://customized_host/path (unchanged)
        location = "hdfs://customized_host/path/to/file";
        host = "";
        result = LocationPath.normalizedHdfsPath(location, host, true);
        Assertions.assertEquals("hdfs://customized_host/path/to/file", result);

        // Test case 5: Empty host parameter with enableOssRootPolicy=false
        // Input: hdfs://host/path
        // Expected: /path
        location = "hdfs://customized_host/path/to/file";
        host = "";
        result = LocationPath.normalizedHdfsPath(location, host, false);
        Assertions.assertEquals("/customized_host/path/to/file", result);

        // Test case 6: hdfs:/// with empty host parameter
        // Input: hdfs:///path
        // Expected: Exception since this format is not supported
        location = "hdfs:///path/to/file";
        host = "";
        boolean exceptionThrown = false;
        try {
            LocationPath.normalizedHdfsPath(location, host, false);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assertions.assertTrue(e.getMessage().contains("Invalid location with empty host"));
        }
        Assertions.assertTrue(exceptionThrown);

        // Test case 7: Non-empty host in URI (regular case)
        // Input: hdfs://existinghost/path
        // Expected: hdfs://existinghost/path (unchanged)
        location = "hdfs://existinghost/path/to/file";
        host = "nameservice";
        result = LocationPath.normalizedHdfsPath(location, host, false);
        Assertions.assertEquals("hdfs://existinghost/path/to/file", result);

        // Test case 8: No valid host name
        // Input: hdfs://hdfs_host/path
        // Expected: hdfs://existinghost/path (unchanged)
        location = "hdfs://hdfs_host/path/to/file";
        host = "nameservice";
        result = LocationPath.normalizedHdfsPath(location, host, false);
        Assertions.assertEquals("hdfs://nameservice/hdfs_host/path/to/file", result);
    }
}
