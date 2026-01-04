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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.catalog.PartitionItem;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

public class RelatedPartitionDescResult {
    // PartitionKeyDesc to relatedTable partition ids(Different partitions may have the same PartitionKeyDesc)
    private Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs;
    private Map<MTMVRelatedTableIf, Map<String, PartitionItem>> items;
    private Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res;

    public RelatedPartitionDescResult() {
        this.descs = Maps.newHashMap();
        this.items = Maps.newHashMap();
        this.res = Maps.newHashMap();
    }

    public Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> getDescs() {
        return descs;
    }

    public void setDescs(
            Map<MTMVRelatedTableIf, Map<PartitionKeyDesc, Set<String>>> descs) {
        this.descs = descs;
    }

    public Map<MTMVRelatedTableIf, Map<String, PartitionItem>> getItems() {
        return items;
    }

    public void setItems(
            Map<MTMVRelatedTableIf, Map<String, PartitionItem>> items) {
        this.items = items;
    }

    public Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> getRes() {
        return res;
    }

    public void setRes(
            Map<PartitionKeyDesc, Map<MTMVRelatedTableIf, Set<String>>> res) {
        this.res = res;
    }
}
