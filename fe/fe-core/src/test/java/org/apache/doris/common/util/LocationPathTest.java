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

        locationPath = new LocationPath("oss://test.oss-dls.aliyuncs.com/path", rangeProps);
        // FE
        Assertions.assertTrue(locationPath.get().startsWith("oss://test.oss-dls.aliyuncs"));
        // BE
        beLocation = locationPath.toStorageLocation().toString();
        Assertions.assertTrue(beLocation.startsWith("oss://test.oss-dls.aliyuncs"));
        Assertions.assertEquals(LocationPath.getFSIdentity(beLocation, null).first, FileSystemType.DFS);

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
}
