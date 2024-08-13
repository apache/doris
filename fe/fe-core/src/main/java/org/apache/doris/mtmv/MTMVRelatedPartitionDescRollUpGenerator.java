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
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Roll up some partitions into one partition
 */
public class MTMVRelatedPartitionDescRollUpGenerator implements MTMVRelatedPartitionDescGeneratorService {

    @Override
    public void apply(MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties,
            RelatedPartitionDescResult lastResult) throws AnalysisException {
        if (mvPartitionInfo.getPartitionType() != MTMVPartitionType.EXPR) {
            return;
        }
        MTMVRelatedTableIf relatedTable = mvPartitionInfo.getRelatedTable();
        PartitionType partitionType = relatedTable.getPartitionType();
        if (partitionType == PartitionType.RANGE) {
            lastResult.setDescs(rollUpRange(lastResult.getDescs(), mvPartitionInfo));
        } else if (partitionType == PartitionType.LIST) {
            lastResult.setDescs(rollUpList(lastResult.getDescs(), mvPartitionInfo, mvProperties));
        } else {
            throw new AnalysisException("only RANGE/LIST partition support roll up");
        }
    }

    /**
     * when related table has 3 partitions:(20200101),(20200102),(20200201)
     * <p>
     * if expr is `date_trunc(month)`
     * then,MTMV will have 2 partitions (20200101,20200102),(20200201)
     * <p>
     * if expr is `date_trunc(year)`
     * then,MTMV will have 1 partitions (20200101,20200102,20200201)
     *
     * @param relatedPartitionDescs
     * @param mvPartitionInfo
     * @return
     * @throws AnalysisException
     */
    public Map<PartitionKeyDesc, Set<String>> rollUpList(Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs,
            MTMVPartitionInfo mvPartitionInfo, Map<String, String> mvProperties) throws AnalysisException {
        Map<String, Set<String>> identityToValues = Maps.newHashMap();
        Map<String, Set<String>> identityToPartitionNames = Maps.newHashMap();
        MTMVPartitionExprService exprSerice = MTMVPartitionExprFactory.getExprService(mvPartitionInfo.getExpr());

        for (Entry<PartitionKeyDesc, Set<String>> entry : relatedPartitionDescs.entrySet()) {
            String rollUpIdentity = exprSerice.getRollUpIdentity(entry.getKey(), mvProperties);
            Preconditions.checkNotNull(rollUpIdentity);
            if (identityToValues.containsKey(rollUpIdentity)) {
                identityToValues.get(rollUpIdentity).addAll(getStringValues(entry.getKey()));
                identityToPartitionNames.get(rollUpIdentity).addAll(entry.getValue());
            } else {
                identityToValues.put(rollUpIdentity, getStringValues(entry.getKey()));
                identityToPartitionNames.put(rollUpIdentity, entry.getValue());
            }
        }
        Map<PartitionKeyDesc, Set<String>> result = Maps.newHashMap();
        for (Entry<String, Set<String>> entry : identityToValues.entrySet()) {
            result.put(PartitionKeyDesc.createIn(getPartitionValues(entry.getValue())),
                    identityToPartitionNames.get(entry.getKey()));
        }
        return result;
    }

    private List<List<PartitionValue>> getPartitionValues(Set<String> strings) {
        List<List<PartitionValue>> inValues = Lists.newArrayList();
        for (String value : strings) {
            inValues.add(Lists.newArrayList(new PartitionValue(value)));
        }
        return inValues;
    }

    private Set<String> getStringValues(PartitionKeyDesc partitionKeyDesc) {
        List<List<PartitionValue>> inValues = partitionKeyDesc.getInValues();
        Set<String> res = Sets.newHashSet();
        for (List<PartitionValue> list : inValues) {
            res.add(list.get(0).getStringValue());
        }
        return res;
    }

    /**
     * when related table has 3 partitions:(20200101-20200102),(20200102-20200103),(20200201-20200202)
     * <p>
     * if expr is `date_trunc(month)`
     * then,MTMV will have 2 partitions (20200101-20200201),(20200101-20200301)
     * <p>
     * if expr is `date_trunc(year)`
     * then,MTMV will have 1 partitions (20200101-20210101)
     *
     * @param relatedPartitionDescs
     * @param mvPartitionInfo
     * @return
     * @throws AnalysisException
     */
    public Map<PartitionKeyDesc, Set<String>> rollUpRange(Map<PartitionKeyDesc, Set<String>> relatedPartitionDescs,
            MTMVPartitionInfo mvPartitionInfo) throws AnalysisException {
        Map<PartitionKeyDesc, Set<String>> result = Maps.newHashMap();
        MTMVPartitionExprService exprSerice = MTMVPartitionExprFactory.getExprService(mvPartitionInfo.getExpr());
        for (Entry<PartitionKeyDesc, Set<String>> entry : relatedPartitionDescs.entrySet()) {
            PartitionKeyDesc rollUpDesc = exprSerice.generateRollUpPartitionKeyDesc(entry.getKey(), mvPartitionInfo);
            if (result.containsKey(rollUpDesc)) {
                result.get(rollUpDesc).addAll(entry.getValue());
            } else {
                result.put(rollUpDesc, entry.getValue());
            }
        }
        return result;
    }
}
