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

package org.apache.doris.fs.obj;

import java.util.List;

public class RemoteObjects {
    private final List<RemoteObject> objectList;

    private final boolean isTruncated;

    private final String continuationToken;

    public RemoteObjects(List<RemoteObject> objectList, boolean isTruncated, String continuationToken) {
        this.objectList = objectList;
        this.isTruncated = isTruncated;
        this.continuationToken = continuationToken;
    }

    public List<RemoteObject> getObjectList() {
        return objectList;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public String getContinuationToken() {
        return continuationToken;
    }

    @Override
    public String toString() {
        return "RemoteObjects{" + "objectList=" + objectList + ", isTruncated=" + isTruncated
                + ", continuationToken='" + continuationToken + '\'' + '}';
    }
}
