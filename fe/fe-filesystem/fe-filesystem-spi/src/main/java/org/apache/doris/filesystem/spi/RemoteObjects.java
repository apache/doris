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

package org.apache.doris.filesystem.spi;

import java.util.List;

/**
 * Result of a list-objects operation, with pagination support.
 */
public final class RemoteObjects {

    private final List<RemoteObject> objectList;
    private final boolean truncated;
    private final String continuationToken;

    public RemoteObjects(List<RemoteObject> objectList, boolean truncated, String continuationToken) {
        this.objectList = objectList == null ? List.of() : List.copyOf(objectList);
        this.truncated = truncated;
        this.continuationToken = continuationToken;
    }

    public List<RemoteObject> getObjectList() {
        return objectList;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public String getContinuationToken() {
        return continuationToken;
    }
}
