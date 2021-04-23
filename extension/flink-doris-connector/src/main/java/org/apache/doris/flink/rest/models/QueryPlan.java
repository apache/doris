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

package org.apache.doris.flink.rest.models;

import java.util.Map;
import java.util.Objects;

public class QueryPlan {
    private int status;
    private String opaqued_query_plan;
    private Map<String, Tablet> partitions;

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getOpaqued_query_plan() {
        return opaqued_query_plan;
    }

    public void setOpaqued_query_plan(String opaqued_query_plan) {
        this.opaqued_query_plan = opaqued_query_plan;
    }

    public Map<String, Tablet> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, Tablet> partitions) {
        this.partitions = partitions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueryPlan queryPlan = (QueryPlan) o;
        return status == queryPlan.status &&
                Objects.equals(opaqued_query_plan, queryPlan.opaqued_query_plan) &&
                Objects.equals(partitions, queryPlan.partitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, opaqued_query_plan, partitions);
    }
}
