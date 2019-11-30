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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprSubstitutionMap;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public abstract class LoadScanNode extends ScanNode {

    public LoadScanNode(PlanNodeId id, TupleDescriptor desc, String planNodeName) {
        super(id, desc, planNodeName);
    }

    protected void initWhereExpr(Expr whereExpr, Analyzer analyzer) throws UserException {
        if (whereExpr == null) {
            return;
        }
        
        Map<String, SlotDescriptor> dstDescMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (SlotDescriptor slotDescriptor : desc.getSlots()) {
            dstDescMap.put(slotDescriptor.getColumn().getName(), slotDescriptor);
        }

        // substitute SlotRef in filter expression
        // where expr must be rewrite first to transfer some predicates(eg: BetweenPredicate to BinaryPredicate)
        whereExpr = analyzer.getExprRewriter().rewrite(whereExpr, analyzer);
        List<SlotRef> slots = Lists.newArrayList();
        whereExpr.collect(SlotRef.class, slots);

        ExprSubstitutionMap smap = new ExprSubstitutionMap();
        for (SlotRef slot : slots) {
            SlotDescriptor slotDesc = dstDescMap.get(slot.getColumnName());
            if (slotDesc == null) {
                throw new UserException("unknown column reference in where statement, reference="
                                                + slot.getColumnName());
            }
            smap.getLhs().add(slot);
            smap.getRhs().add(new SlotRef(slotDesc));
        }
        whereExpr = whereExpr.clone(smap);
        whereExpr.analyze(analyzer);
        if (whereExpr.getType() != Type.BOOLEAN) {
            throw new UserException("where statement is not a valid statement return bool");
        }
        addConjuncts(whereExpr.getConjuncts());
    }

    protected void checkBitmapCompatibility(SlotDescriptor slotDesc, Expr expr) throws AnalysisException {
        boolean isCompatible = true;
        if (slotDesc.getColumn().getAggregationType() == AggregateType.BITMAP_UNION) {
            if (!(expr instanceof FunctionCallExpr)) {
                isCompatible = false;
            } else {
                FunctionCallExpr fn = (FunctionCallExpr) expr;
                if (!fn.getFnName().getFunction().equalsIgnoreCase(FunctionSet.TO_BITMAP)
                        && !fn.getFnName().getFunction().equalsIgnoreCase(FunctionSet.BITMAP_EMPTY)) {
                    isCompatible = false;
                }
            }
        }
        if (!isCompatible) {
            throw new AnalysisException("bitmap column must use to_bitmap or empty_bitmap function, like "
                    + slotDesc.getColumn().getName() + "=to_bitmap(xxx)"
                    + slotDesc.getColumn().getName() + "=bitmap_empty()");
        }
    }

}
