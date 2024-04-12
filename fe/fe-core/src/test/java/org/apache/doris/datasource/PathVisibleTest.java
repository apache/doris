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

package org.apache.doris.datasource;

import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

public class PathVisibleTest {
    @Test
    public void shouldReturnFalseWhenPathIsNull() {
        Assert.assertFalse(FileCacheValue.isFileVisible(null));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("s3://visible/.hidden/path")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("/visible/.hidden/path")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("hdfs://visible/path/.file")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("/visible/path/_temporary_xx")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("/visible/path/_imapala_insert_staging")));

        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("/visible//.hidden/path")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("s3://visible/.hidden/path")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("///visible/path/.file")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("/visible/path///_temporary_xx")));
        Assert.assertFalse(FileCacheValue.isFileVisible(new Path("hdfs://visible//path/_imapala_insert_staging")));

        Assert.assertTrue(FileCacheValue.isFileVisible(new Path("s3://visible/path")));
        Assert.assertTrue(FileCacheValue.isFileVisible(new Path("path")));
        Assert.assertTrue(FileCacheValue.isFileVisible(new Path("hdfs://visible/path./1.txt")));
        Assert.assertTrue(FileCacheValue.isFileVisible(new Path("/1.txt")));
    }
}
