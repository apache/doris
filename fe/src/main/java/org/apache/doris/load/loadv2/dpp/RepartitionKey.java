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

package org.apache.doris.load.loadv2.dpp;

import java.io.Serializable;
import java.util.Objects;

public class RepartitionKey implements Comparable<RepartitionKey>, Serializable {
    public DppColumns dppKey;
    // format: partitionId_bucketId
    public String bucketKey;

    @Override
    public int compareTo(RepartitionKey key) {
        return bucketKey.compareTo(key.bucketKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RepartitionKey repartitionKey = (RepartitionKey)o;
        if (!repartitionKey.equals(repartitionKey.bucketKey)) {
            return false;
        }
        return dppKey.equals(repartitionKey.dppKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketKey);
    }

    @Override
    public String toString() {
        return "RepartitionKey{" +
                "repartitionKey=" + bucketKey.toString() +
                ",Dpp Keys=" + dppKey.toString() +
                '}';
    }
}
