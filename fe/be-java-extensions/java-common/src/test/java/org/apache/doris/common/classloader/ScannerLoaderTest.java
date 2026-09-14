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
    public void testCleanAllCachesByFunctionSignatureWithoutFunctionId() {
        long firstFunctionId = 10003;
        long secondFunctionId = 10004;
        long otherFunctionId = 10005;
        String functionSignature = "legacy_function(INT)";
        String otherFunctionSignature = "other_function(INT)";
        UdfClassCache firstCache = new UdfClassCache();
        UdfClassCache secondCache = new UdfClassCache();
        UdfClassCache otherCache = new UdfClassCache();
        ScannerLoader loader = new ScannerLoader();

        try {
            ScannerLoader.cacheClassLoader(functionSignature, firstFunctionId, firstCache, 0);
            ScannerLoader.cacheClassLoader(functionSignature, secondFunctionId, secondCache, 0);
            ScannerLoader.cacheClassLoader(otherFunctionSignature, otherFunctionId, otherCache, 0);

            loader.cleanUdfClassLoader(functionSignature, 0);

            Assert.assertNull(ScannerLoader.getUdfClassLoader(firstFunctionId));
            Assert.assertNull(ScannerLoader.getUdfClassLoader(secondFunctionId));
            Assert.assertSame(otherCache, ScannerLoader.getUdfClassLoader(otherFunctionId));
        } finally {
            loader.cleanUdfClassLoader(functionSignature, firstFunctionId);
            loader.cleanUdfClassLoader(functionSignature, secondFunctionId);
            loader.cleanUdfClassLoader(otherFunctionSignature, otherFunctionId);
        }
    }
}
