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

package org.apache.doris.load.loadv2;

import org.apache.doris.thrift.TBrokerFileStatus;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class BrokerPendingTaskAttachment extends TaskAttachment {

    // table id -> file status
    private Map<Long, List<List<TBrokerFileStatus>>> fileStatusMap = Maps.newHashMap();
    // table id -> total file num
    private Map<Long, Integer> fileNumMap = Maps.newHashMap();

    public BrokerPendingTaskAttachment(long taskId) {
        super(taskId);
    }

    public void addFileStatus(long tableId, List<List<TBrokerFileStatus>> fileStatusList) {
        fileStatusMap.put(tableId, fileStatusList);
        fileNumMap.put(tableId, fileStatusList.stream().mapToInt(entity -> entity.size()).sum());
    }

    public List<List<TBrokerFileStatus>> getFileStatusByTable(long tableId) {
        return fileStatusMap.get(tableId);
    }

    public int getFileNumByTable(long tableId) {
        return fileNumMap.get(tableId);
    }
}
