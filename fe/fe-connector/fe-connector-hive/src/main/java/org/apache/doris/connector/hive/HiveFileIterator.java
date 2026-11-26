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

package org.apache.doris.connector.hive;

import java.util.Iterator;

/** A scan-owned file cursor. Advancing and closing are serialized by the split producer. */
interface HiveFileIterator extends Iterator<HiveFileStatus>, AutoCloseable {
    @Override
    void close();

    static HiveFileIterator of(Iterator<HiveFileStatus> files) {
        class Cursor implements HiveFileIterator {
            private Iterator<HiveFileStatus> delegate;

            Cursor(Iterator<HiveFileStatus> delegate) {
                this.delegate = delegate;
            }

            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public HiveFileStatus next() {
                return delegate.next();
            }

            @Override
            public void close() {
                delegate = java.util.Collections.emptyIterator();
            }
        }

        return new Cursor(files);
    }
}
