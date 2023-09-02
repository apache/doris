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

public class RemoteObject {
    private final String key;
    private final String relativePath;
    private final String etag;
    private final long size;

    public RemoteObject(String key, String relativePath, String etag, long size) {
        this.key = key;
        this.relativePath = relativePath;
        this.etag = etag;
        this.size = size;
    }

    public String getKey() {
        return key;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public String getEtag() {
        return etag;
    }

    public long getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "RemoteObject{" + "key='" + key + '\'' + ", relativePath='" + relativePath + '\'' + ", etag='" + etag
                + '\'' + ", size=" + size + '}';
    }
}
