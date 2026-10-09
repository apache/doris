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

package org.apache.doris.paimon;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class NativeGcsFileIOTest {
    @TempDir
    public java.nio.file.Path directory;

    @Test
    public void aliasesUseNativeFilesystemBeforeAndAfterSerialization() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.gs.impl", LocalGcsFileSystem.class.getName());
        conf.setBoolean("fs.gs.impl.disable.cache", true);
        conf.set("test.gcs.root", directory.toString());
        conf.set("fs.gs.auth.type", "APPLICATION_DEFAULT");
        NativeGcsFileIO original = new NativeGcsFileIO(new Path("s3a://bucket/warehouse"));
        original.configure(CatalogContext.create(new Options(), conf));
        NativeGcsFileIO restored = InstantiationUtil.deserializeObject(
                InstantiationUtil.serializeObject(original), getClass().getClassLoader());
        for (NativeGcsFileIO fileIO : new NativeGcsFileIO[] {original, restored}) {
            Assertions.assertEquals("APPLICATION_DEFAULT", fileIO.hadoopConf().get("fs.gs.auth.type"));
            Assertions.assertTrue(fileIO.isObjectStore());
            for (String scheme : new String[] {"s3", "s3a", "gs"}) {
                Path folder = new Path(scheme + "://bucket/" + scheme);
                Path file = new Path(folder, "manifest");
                Assertions.assertTrue(fileIO.mkdirs(folder));
                fileIO.writeFile(file, "private-gcs-data", true);
                Assertions.assertEquals("private-gcs-data", fileIO.readFileUtf8(file));
                Assertions.assertTrue(fileIO.exists(file));
                Assertions.assertEquals(16, fileIO.getFileStatus(file).getLen());
                Assertions.assertEquals(1, fileIO.listStatus(folder).length);
                Assertions.assertTrue(fileIO.listFilesIterative(folder, true).hasNext());
                Path target = new Path("s3a://bucket/" + scheme + "/renamed");
                Assertions.assertTrue(fileIO.rename(file, target));
                Assertions.assertTrue(fileIO.delete(target, false));
            }
        }
    }

    @Test
    public void normalizationPreservesObjectKeyAndOtherSchemes() {
        Assertions.assertEquals("gs://bucket/a%20b/part-1",
                NativeGcsFileIO.normalize(new Path("s3a://bucket/a%20b/part-1")).toString());
        for (String uri : new String[] {"gs://bucket/key", "hdfs://namenode/key", "file:/tmp/key"}) {
            Path path = new Path(uri);
            Assertions.assertSame(path, NativeGcsFileIO.normalize(path));
        }
    }

    /** Local backing storage that rejects any path not normalized to gs before Hadoop dispatch. */
    public static class LocalGcsFileSystem extends RawLocalFileSystem {
        @Override
        public URI getUri() {
            return URI.create("gs://bucket");
        }

        @Override
        public FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
            FileStatus status = super.getFileStatus(path);
            return new FileStatus(status.getLen(), status.isDirectory(), 1, 4096,
                    status.getModificationTime(), path);
        }

        @Override
        public BlockLocation[] getFileBlockLocations(FileStatus status, long start, long length) {
            return new BlockLocation[0];
        }

        @Override
        public File pathToFile(org.apache.hadoop.fs.Path path) {
            Assertions.assertEquals("gs", path.toUri().getScheme());
            return new File(getConf().get("test.gcs.root"), path.toUri().getPath());
        }
    }
}
