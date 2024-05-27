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

package org.apache.doris.regression.util

import groovy.transform.CompileStatic

@CompileStatic
class ReusableIterator<T> implements CloseableIterator<T> {
    private CloseableIterator<T> it
    private T next
    private boolean cached
    private int currentId

    ReusableIterator(CloseableIterator<T> it) {
        this.it = it
        this.currentId = 0
    }

    @Override
    void close() throws IOException {
        it.close()
    }

    @Override
    boolean hasNext() {
        if (!cached) {
            if (it.hasNext()) {
                next = it.next()
                cached = true
                return true
            } else {
                return false
            }
        } else {
            return true
        }
    }

    T preRead() {
        return next
    }

    @Override
    T next() {
        if (hasNext()) {
            cached = false
            currentId++
            return next
        }
        throw new NoSuchElementException()
    }

    int getCurrentId() {
        return currentId
    }
}
