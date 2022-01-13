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

package org.apache.doris.common.proc;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.resource.Tag;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/*
 * show proc "/colocation_group/group_name";
 */
public class ColocationGroupBackendSeqsProcNode implements ProcNodeInterface {
    private Map<Tag, List<List<Long>>> backendsSeq;

    public ColocationGroupBackendSeqsProcNode(Map<Tag, List<List<Long>>> backendsSeq) {
        this.backendsSeq = backendsSeq;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        List<String> titleNames = Lists.newArrayList();
        titleNames.add("BucketIndex");
        int bucketNum = 0;
        for (Tag tag : backendsSeq.keySet()) {
            titleNames.add(tag.toString());
            if (bucketNum == 0) {
                bucketNum = backendsSeq.get(tag).size();
            } else if (bucketNum != backendsSeq.get(tag).size()) {
                throw new AnalysisException("Invalid bucket number: " + bucketNum + " vs. " + backendsSeq.get(tag).size());
            }
        }
        result.setNames(titleNames);
        for (int i = 0; i < bucketNum; i++) {
            List<String> info = Lists.newArrayList();
            info.add(String.valueOf(i)); // bucket index
            for (Tag tag : backendsSeq.keySet()) {
                List<List<Long>> bucketBackends = backendsSeq.get(tag);
                info.add(Joiner.on(", ").join(bucketBackends.get(i)));
            }
            result.addRow(info);
        }
        return result;
    }

}
