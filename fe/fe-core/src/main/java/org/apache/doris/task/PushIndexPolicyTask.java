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

package org.apache.doris.task;

import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.thrift.TIndexPolicy;
import org.apache.doris.thrift.TPushIndexPolicyReq;
import org.apache.doris.thrift.TTaskType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

public class PushIndexPolicyTask extends AgentTask {
    private static final Logger LOG = LogManager.getLogger(PushIndexPolicyTask.class);

    private List<IndexPolicy> indexPolicys;
    private List<Long> droppedIndexPolicys;

    public PushIndexPolicyTask(long backendId, List<IndexPolicy> indexPolicys,
                                 List<Long> droppedIndexPolicys) {
        super(null, backendId, TTaskType.PUSH_INDEX_POLICY, -1, -1, -1, -1, -1, -1, -1);
        this.indexPolicys = indexPolicys;
        this.droppedIndexPolicys = droppedIndexPolicys;
    }

    public TPushIndexPolicyReq toThrift() {
        LOG.debug("Starting to convert index policies to thrift format");
        TPushIndexPolicyReq req = new TPushIndexPolicyReq();

        List<TIndexPolicy> tPolicys = indexPolicys.stream()
                .map(p -> new TIndexPolicy()
                        .setId(p.getId())
                        .setName(p.getName())
                        .setType(p.getType().toThrift())
                        .setProperties(p.getProperties()))
                .collect(Collectors.toList());

        req.setIndexPolicys(tPolicys);
        req.setDroppedIndexPolicys(droppedIndexPolicys);

        LOG.debug("Successfully converted {} index policies and {} dropped policy ids to thrift",
                tPolicys.size(), droppedIndexPolicys.size());
        return req;
    }
}
