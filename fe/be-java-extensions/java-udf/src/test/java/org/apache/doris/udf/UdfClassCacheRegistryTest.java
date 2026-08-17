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

package org.apache.doris.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * What a statically loaded function's compiled classes are filed under.
 *
 * <p>Each case here is a way the signature - the key this used to be - identifies the wrong thing.
 * None of them is visible in a unit test of the executor: they need two functions, or a DROP, to
 * show up at all, which is why they lived through several rewrites of this cache.
 */
public class UdfClassCacheRegistryTest {

    /**
     * Two functions of the same name in different databases are two functions. The signature has no
     * database in it, so they used to share one compiled class - whichever ran first - and dropping
     * either dropped both.
     */
    @Test
    public void differentFunctionsWithOneSignatureGetDifferentKeys() {
        Assertions.assertNotEquals(UdfClassCacheRegistry.cacheKey(11L, "f(INT)"),
                UdfClassCacheRegistry.cacheKey(12L, "f(INT)"));
    }

    /**
     * The same function is the same entry however its signature is spelled. FE appends {@code "..."}
     * to a variadic signature when it describes a function to execute and does not when it drops
     * one, so a variadic function's entry could never be invalidated: DROP plus CREATE went on
     * running the old code until the BE restarted.
     */
    @Test
    public void oneFunctionKeepsOneKeyAcrossSignatureSpellings() {
        Assertions.assertEquals(UdfClassCacheRegistry.cacheKey(11L, "f(INT...)"),
                UdfClassCacheRegistry.cacheKey(11L, "f(INT)"));
    }

    /** A re-created function is a new function, so it cannot inherit its predecessor's classes. */
    @Test
    public void recreatedFunctionDoesNotReuseTheOldKey() {
        Assertions.assertNotEquals(UdfClassCacheRegistry.cacheKey(11L, "f(INT)"),
                UdfClassCacheRegistry.cacheKey(99L, "f(INT)"));
    }

    /** With no id there is nothing better than the signature, which is what it always used. */
    @Test
    public void withoutAnIdTheSignatureIsStillTheKey() {
        Assertions.assertEquals(UdfClassCacheRegistry.cacheKey(0L, "f(INT)"),
                UdfClassCacheRegistry.cacheKey(-1L, "f(INT)"));
        Assertions.assertNotEquals(UdfClassCacheRegistry.cacheKey(0L, "f(INT)"),
                UdfClassCacheRegistry.cacheKey(0L, "g(INT)"));
    }

    /** An id-derived key and a signature-derived one must never collide. */
    @Test
    public void idAndSignatureKeysCannotCollide() {
        Assertions.assertNotEquals(UdfClassCacheRegistry.cacheKey(7L, "f(INT)"),
                UdfClassCacheRegistry.cacheKey(0L, "id=7"));
    }
}
