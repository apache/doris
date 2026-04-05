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

package org.apache.doris.filesystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class SimpleRemoteIteratorTest {

    @Test
    void delegatesToUnderlyingIterator() throws FileSystemIOException {
        FileEntry e1 = new FileEntry(Location.of("s3://b/f1"), 10, false, 0, null);
        FileEntry e2 = new FileEntry(Location.of("s3://b/f2"), 20, false, 0, null);
        Iterator<FileEntry> underlying = List.of(e1, e2).iterator();

        SimpleRemoteIterator iter = new SimpleRemoteIterator(underlying);
        Assertions.assertTrue(iter.hasNext());
        Assertions.assertEquals(e1, iter.next());
        Assertions.assertTrue(iter.hasNext());
        Assertions.assertEquals(e2, iter.next());
        Assertions.assertFalse(iter.hasNext());
    }

    @Test
    void emptyIterator() throws FileSystemIOException {
        SimpleRemoteIterator iter = new SimpleRemoteIterator(List.<FileEntry>of().iterator());
        Assertions.assertFalse(iter.hasNext());
    }

    @Test
    void nextOnExhaustedIteratorThrows() throws FileSystemIOException {
        SimpleRemoteIterator iter = new SimpleRemoteIterator(List.<FileEntry>of().iterator());
        Assertions.assertFalse(iter.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    void nullIteratorThrowsNPE() {
        Assertions.assertThrows(NullPointerException.class, () -> new SimpleRemoteIterator(null));
    }
}
