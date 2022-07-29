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

package org.apache.doris.nereids.properties;

import java.util.List;
/**
 * Represents order keys and limit and offset
 */

public class QueryOrganization {

    private List<OrderKey> orderKeys;
    private long limit;
    private long offset;

    public QueryOrganization(List<OrderKey> orderKeys, long limit, long offset) {
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
    }

    public long getLimit() {
        return limit;
    }

    public long getOffset() {
        return offset;
    }

    public void setOrderKeys(List<OrderKey> orderKeys) {
        this.orderKeys = orderKeys;
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    public boolean hasLimitClause() {
        return limit != Long.MAX_VALUE;
    }

}
