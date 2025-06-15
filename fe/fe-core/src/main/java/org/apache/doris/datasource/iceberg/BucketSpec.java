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

package org.apache.doris.datasource.iceberg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class BucketSpec {
    private final int numBuckets;
    private final List<String> bucketColumnNames;
    private final List<String> sortColumnNames;

    public BucketSpec(int numBuckets, List<String> bucketColumnNames, List<String> sortColumnNames) {
        this.numBuckets = numBuckets;
        this.bucketColumnNames = Collections.unmodifiableList(new ArrayList<>(bucketColumnNames));
        this.sortColumnNames = Collections.unmodifiableList(new ArrayList<>(sortColumnNames));
    }

    public int numBuckets() {
        return numBuckets;
    }

    public List<String> bucketColumnNames() {
        return bucketColumnNames;
    }

    public List<String> sortColumnNames() {
        return sortColumnNames;
    }

    @Override
    public String toString() {
        return String.format("BucketSpec{numBuckets=%d, bucketColumnNames=%s, sortColumnNames=%s}",
            numBuckets, bucketColumnNames, sortColumnNames);
    }
}
