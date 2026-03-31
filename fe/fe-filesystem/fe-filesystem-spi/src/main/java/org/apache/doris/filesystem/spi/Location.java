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

import java.util.Objects;

/**
 * Immutable, typed URI wrapper replacing raw String path parameters.
 */
public final class Location {

    private final String uri;

    private Location(String uri) {
        this.uri = Objects.requireNonNull(uri, "uri must not be null");
    }

    public static Location of(String uri) {
        return new Location(uri);
    }

    public String uri() {
        return uri;
    }

    public String scheme() {
        int idx = uri.indexOf("://");
        return idx < 0 ? "" : uri.substring(0, idx);
    }

    public String withoutScheme() {
        int idx = uri.indexOf("://");
        return idx < 0 ? uri : uri.substring(idx + 3);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Location)) {
            return false;
        }
        return uri.equals(((Location) o).uri);
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    @Override
    public String toString() {
        return uri;
    }
}
