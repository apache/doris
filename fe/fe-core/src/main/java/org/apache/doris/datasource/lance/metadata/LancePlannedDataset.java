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

package org.apache.doris.datasource.lance.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * What the plan keeps of one Lance dataset once it has been opened: its URI, the version one open
 * pinned, and the fragments that version carries.
 */
public final class LancePlannedDataset {
    private final String uri;
    private final long version;
    private final List<LanceFragmentInfo> fragments;

    public LancePlannedDataset(String uri, long version, List<LanceFragmentInfo> fragments) {
        this.uri = uri;
        this.version = version;
        this.fragments = Collections.unmodifiableList(new ArrayList<>(fragments));
    }

    public String getUri() {
        return uri;
    }

    public long getVersion() {
        return version;
    }

    public List<LanceFragmentInfo> getFragments() {
        return fragments;
    }
}
