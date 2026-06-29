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

package org.apache.doris.datasource.metacache;

import org.apache.doris.common.Pair;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

public class NameCacheValueTest {

    @Test
    public void testCopyOnWriteKeepsOriginalSnapshot() {
        NameCacheValue original = NameCacheValue.of(ImmutableList.of(Pair.of("RemoteA", "LocalA")));
        NameCacheValue updated = original.withName("RemoteB", "LocalB");

        Assert.assertEquals("RemoteA", original.remoteNameOfLocalName("LocalA"));
        Assert.assertNull(original.remoteNameOfLocalName("LocalB"));
        Assert.assertEquals("RemoteB", updated.remoteNameOfLocalName("LocalB"));
    }

    @Test
    public void testCaseInsensitiveLookupReturnsRemoteName() {
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(
                Pair.of("RemoteA", "LocalA"),
                Pair.of("RemoteB", "RemoteB")));

        Assert.assertEquals("RemoteA", names.remoteNameForCaseInsensitiveLookup("remotea"));
        Assert.assertEquals("RemoteB", names.remoteNameForCaseInsensitiveLookup("REMOTEB"));
    }

    @Test
    public void testWithNameRejectsRemoteNameConflict() {
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(Pair.of("RemoteA", "LocalA")));

        IllegalArgumentException exception = Assert.assertThrows(
                IllegalArgumentException.class, () -> names.withName("RemoteA", "LocalB"));
        Assert.assertTrue(exception.getMessage().contains("remote name already maps"));
    }

    @Test
    public void testWithoutLocalNameRemovesMapping() {
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(
                Pair.of("RemoteA", "LocalA"),
                Pair.of("RemoteB", "LocalB")));

        NameCacheValue updated = names.withoutLocalName("LocalA");
        Assert.assertFalse(updated.containsLocalName("LocalA"));
        Assert.assertEquals("RemoteB", updated.remoteNameOfLocalName("LocalB"));
    }
}
