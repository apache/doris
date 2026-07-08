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
    public void testSourcePairMutationDoesNotChangeSnapshot() {
        Pair<String, String> pair = Pair.of("RemoteA", "LocalA");
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(pair));

        // Mutating the original Pair must not affect the published snapshot or its lower-case index.
        pair.first = "RemoteB";
        pair.second = "LocalB";

        Assert.assertEquals("RemoteA", names.remoteNameOfLocalName("LocalA"));
        Assert.assertEquals("RemoteA", names.remoteNameForCaseInsensitiveLookup("remotea"));
        Assert.assertNull(names.remoteNameOfLocalName("LocalB"));
    }

    @Test
    public void testReturnedPairMutationDoesNotChangeSnapshot() {
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(Pair.of("RemoteA", "LocalA")));

        // Return fresh Pair copies so readers cannot mutate the shared snapshot in place.
        Pair<String, String> returned = names.names().get(0);
        returned.first = "RemoteB";
        returned.second = "LocalB";

        Assert.assertEquals("RemoteA", names.remoteNameOfLocalName("LocalA"));
        Assert.assertEquals("RemoteA", names.remoteNameForCaseInsensitiveLookup("remotea"));
        Assert.assertNull(names.remoteNameOfLocalName("LocalB"));
    }

    @Test
    public void testLocalNamesExposeImmutableReadOnlyView() {
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(
                Pair.of("RemoteA", "LocalA"),
                Pair.of("RemoteB", "LocalB")));

        Assert.assertEquals(ImmutableList.of("LocalA", "LocalB"), names.localNames());
        Assert.assertThrows(UnsupportedOperationException.class, () -> names.localNames().add("LocalC"));
    }

    @Test
    public void testCaseSensitiveNamesCanShareLowerCaseKey() {
        // Keep both exact-name mappings and let the lower-case index follow snapshot insertion order.
        NameCacheValue names = NameCacheValue.of(ImmutableList.of(
                Pair.of("Foo", "Foo"),
                Pair.of("foo", "foo")));

        Assert.assertEquals("Foo", names.remoteNameOfLocalName("Foo"));
        Assert.assertEquals("foo", names.remoteNameOfLocalName("foo"));
        Assert.assertEquals("foo", names.remoteNameForCaseInsensitiveLookup("FOO"));
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
