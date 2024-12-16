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

package org.apache.doris.datasource.paimon;

import org.apache.doris.catalog.PartitionItem;

import com.google.common.collect.Maps;

import java.util.Map;

public class PaimonPartitionInfo {
    private final Map<String, PartitionItem> nameToPartitionItem;
    private final Map<String, PaimonPartition> nameToPartition;

    public PaimonPartitionInfo() {
        this.nameToPartitionItem = Maps.newHashMap();
        this.nameToPartition = Maps.newHashMap();
    }

    public PaimonPartitionInfo(Map<String, PartitionItem> nameToPartitionItem,
            Map<String, PaimonPartition> nameToPartition) {
        this.nameToPartitionItem = nameToPartitionItem;
        this.nameToPartition = nameToPartition;
    }

    public Map<String, PartitionItem> getNameToPartitionItem() {
        return nameToPartitionItem;
    }

    public Map<String, PaimonPartition> getNameToPartition() {
        return nameToPartition;
    }
}
