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

package org.apache.doris.analysis;

import com.google.common.collect.Lists;
import java.util.List;

// to describe the key list partition's information in create table stmt
public class MultiPartitionDesc {


    private RangePartitionDesc rangePartitionDesc;

    public MultiPartitionDesc(List<String> partitionColNames,
                              List<MultiPartition> multiPartitionDescs) {
        this.rangePartitionDesc = new RangePartitionDesc(partitionColNames,multiPartitionDescsToSinglePartitionDescs(multiPartitionDescs));
    }


    public RangePartitionDesc getRangePartitionDesc() {
        return this.rangePartitionDesc;
    }

    private List<SinglePartitionDesc> multiPartitionDescsToSinglePartitionDescs(List<MultiPartition> multiPartitions){
        List<SinglePartitionDesc> res = Lists.newArrayList();
        for (MultiPartition multiPartition : multiPartitions) {
            res.addAll(multiPartition.getSinglePartitionDescList());
        }
        return res;
    }


}
