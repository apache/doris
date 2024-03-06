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
import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MTMVExprUtil {
    public static Map<PartitionKeyDesc, Set<Long>> rollUp(Map<PartitionKeyDesc, Set<Long>> relatedPartitionDescs,
            MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        MTMVRelatedTableIf relatedTable = mvPartitionInfo.getRelatedTable();
        if (relatedTable.getPartitionType() == PartitionType.RANGE) {
            return rollUpRange(relatedPartitionDescs, mvPartitionInfo);
        } else {
            return rollUpList(relatedPartitionDescs, mvPartitionInfo);
        }
    }

    public static Map<PartitionKeyDesc, Set<Long>> rollUpList(Map<PartitionKeyDesc, Set<Long>> relatedPartitionDescs,
            MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        Map<String, Set<String>> rollUpIdentitys = Maps.newHashMap();
        Map<String, Set<Long>> identitysToPartitionIds = Maps.newHashMap();
        MTMVPartitionExprService exprSerice = MTMVPartitionExprFactory.getExprSerice(mvPartitionInfo.getExpr());
        for (Entry<PartitionKeyDesc, Set<Long>> entry : relatedPartitionDescs.entrySet()) {
            String rollUpIdentity = exprSerice.getRollUpIdentity(entry.getKey());
            if (rollUpIdentitys.containsKey(rollUpIdentity)) {
                rollUpIdentitys.get(rollUpIdentity).addAll(getInValues(entry.getKey()));
                identitysToPartitionIds.get(rollUpIdentity).addAll(entry.getValue());
            } else {
                rollUpIdentitys.put(rollUpIdentity, getInValues(entry.getKey()));
                identitysToPartitionIds.put(rollUpIdentity, entry.getValue());
            }
        }
        Map<PartitionKeyDesc, Set<Long>> result = Maps.newHashMap();
        for (Entry<String, Set<String>> entry : rollUpIdentitys.entrySet()) {
            List<List<PartitionValue>> inValues = Lists.newArrayList();
            for (String value : entry.getValue()) {
                inValues.add(Lists.newArrayList(new PartitionValue(value)));
            }
            result.put(PartitionKeyDesc.createIn(inValues), identitysToPartitionIds.get(entry.getKey()));
        }
        return result;
    }

    public static Map<PartitionKeyDesc, Set<Long>> rollUpRange(Map<PartitionKeyDesc, Set<Long>> relatedPartitionDescs,
            MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        Map<PartitionKeyDesc, Set<Long>> result = Maps.newHashMap();
        MTMVPartitionExprService exprSerice = MTMVPartitionExprFactory.getExprSerice(mvPartitionInfo.getExpr());
        for (Entry<PartitionKeyDesc, Set<Long>> entry : relatedPartitionDescs.entrySet()) {
            PartitionKeyDesc rollUpDesc = exprSerice.generateRollUpPartitionKeyDesc(entry.getKey(), mvPartitionInfo);
            if (result.containsKey(rollUpDesc)) {
                result.get(rollUpDesc).addAll(entry.getValue());
            } else {
                result.put(rollUpDesc, entry.getValue());
            }
        }
        return result;
    }

    private static Set<String> getInValues(PartitionKeyDesc partitionKeyDesc) {
        List<List<PartitionValue>> inValues = partitionKeyDesc.getInValues();
        Set<String> res = Sets.newHashSet();
        for (List<PartitionValue> list : inValues) {
            res.add(list.get(0).getStringValue());
        }
        return res;
    }
}

