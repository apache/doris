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

package org.apache.doris.common.classloader;

import org.apache.doris.common.jni.utils.UdfClassCache;

import org.junit.Assert;
import org.junit.Test;

public class ScannerLoaderTest {
    @Test
    public void testCleanCacheByFunctionId() {
        long oldFunctionId = 10001;
        long newFunctionId = 10002;
        String functionSignature = "recreated_function(INT)";
        UdfClassCache oldCache = new UdfClassCache();
        UdfClassCache newCache = new UdfClassCache();
        ScannerLoader loader = new ScannerLoader();

        try {
            ScannerLoader.cacheClassLoader(functionSignature, oldFunctionId, oldCache, 0);
            ScannerLoader.cacheClassLoader(functionSignature, newFunctionId, newCache, 0);

            loader.cleanUdfClassLoader(functionSignature, oldFunctionId);

            Assert.assertNull(ScannerLoader.getUdfClassLoader(oldFunctionId));
            Assert.assertSame(newCache, ScannerLoader.getUdfClassLoader(newFunctionId));
        } finally {
            loader.cleanUdfClassLoader(functionSignature, oldFunctionId);
            loader.cleanUdfClassLoader(functionSignature, newFunctionId);
        }
    }

    @Test
    public void testOldFeCanCleanCacheByFunctionSignature() {
        long functionId = 10003;
        String functionSignature = "legacy_function(INT)";
        UdfClassCache cache = new UdfClassCache();
        ScannerLoader loader = new ScannerLoader();

        try {
            ScannerLoader.cacheClassLoader(functionSignature, functionId, cache, 0);

            loader.cleanUdfClassLoader(functionSignature, 0);

            Assert.assertNull(ScannerLoader.getUdfClassLoader(functionId));
        } finally {
            loader.cleanUdfClassLoader(functionSignature, functionId);
        }
    }
}
