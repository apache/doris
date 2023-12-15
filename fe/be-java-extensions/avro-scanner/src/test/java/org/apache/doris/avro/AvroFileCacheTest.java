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

package org.apache.doris.avro;

import org.apache.doris.avro.AvroFileCache.AvroFileCacheKey;
import org.apache.doris.avro.AvroFileCache.AvroFileMeta;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AvroFileCacheTest {
    private String schemaTxt;

    @Before
    public void setUp() {
        schemaTxt = "[{\"name\":\"aBoolean\",\"type\":2,\"childColumns\":null},{\"name\":\"aInt\",\"type\":5,"
            + "\"childColumns\":null},{\"name\":\"aLong\",\"type\":6,\"childColumns\":null},{\"name\":\""
            + "aFloat\",\"type\":7,\"childColumns\":null},{\"name\":\"aDouble\",\"type\":8,\"childColumns\""
            + ":null},{\"name\":\"aString\",\"type\":23,\"childColumns\":null},{\"name\":\"aBytes\",\"type\""
            + ":11,\"childColumns\":null},{\"name\":\"aFixed\",\"type\":11,\"childColumns\":null},{\"name\""
            + ":\"anArray\",\"type\":20,\"childColumns\":[{\"name\":null,\"type\":5,\"childColumns\":null}]}"
            + ",{\"name\":\"aMap\",\"type\":21,\"childColumns\":[{\"name\":null,\"type\":23,\"childColumns\""
            + ":null},{\"name\":null,\"type\":5,\"childColumns\":null}]},{\"name\":\"anEnum\",\"type\":23"
            + ",\"childColumns\":null},{\"name\":\"aRecord\",\"type\":22,\"childColumns\":[{\"name\":\"a\","
            + "\"type\":5,\"childColumns\":null},{\"name\":\"b\",\"type\":8,\"childColumns\":null},{\"name\":"
            + "\"c\",\"type\":23,\"childColumns\":null}]},{\"name\":\"aUnion\",\"type\":22,\"childColumns\":"
            + "[{\"name\":\"string\",\"type\":23,\"childColumns\":null}]}]\n";
    }

    @Test
    public void testAddCache() {
        AvroFileMeta fileMeta = new AvroFileMeta(schemaTxt);
        AvroFileCacheKey key1 = new AvroFileCacheKey("FILE_LOCAL", "local-test-path");
        AvroFileCacheKey key2 = new AvroFileCacheKey("FILE_LOCAL", "local-test-path");
        AvroFileCacheKey key3 = new AvroFileCacheKey("FILE_LOCAL", "local-test-path");
        AvroFileCache.addFileMeta(key1, fileMeta);

        AvroFileCache.addFileMeta(key2, fileMeta);
        AvroFileCache.addFileMeta(key3, fileMeta);

        AvroFileMeta result = AvroFileCache.getAvroFileMeta(key1);
        Assert.assertEquals(result, fileMeta);

        result = AvroFileCache.getAvroFileMeta(key2);
        Assert.assertEquals(result, fileMeta);

        result = AvroFileCache.getAvroFileMeta(key3);
        Assert.assertEquals(result, fileMeta);

        AvroFileCache.invalidateFileCache(key3);
        result = AvroFileCache.getAvroFileMeta(key3);
        Assert.assertEquals(result, null);
    }
}
