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


import com.google.common.base.Preconditions;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.qe.dict.GlobalDictManger;
import org.apache.doris.qe.dict.IDict;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class DictPlanner {

    private final DecodeContext context;

    private List<Expr> resultExprList;

    public DictPlanner(PlannerContext ctx,
                       DescriptorTable tableDesc,
                       Analyzer analyzer,
                       List<Expr> resultExprList) {
        this.context = new DecodeContext(ctx, tableDesc, analyzer);
        this.resultExprList = resultExprList;
    }

    public PlanNode plan(PlanNode plan) {

        findDictCodableSlot(plan);
        // for now, we only support the dict column used for aggregation, if it was used in
        // some other expressions then we simply disable the dict optimization on it
        filterSupportedDictSlot(plan);

        if (context.needEncode()) {

            updateNodes(plan);

            generateDecodeNode(null, plan);

            plan = insertDecodeNode(null, plan);

            handleResultExpr(plan);
        }
        return plan;

    }

    private void handleResultExpr(PlanNode plan) {
        List<TupleId> originTupleIdList = plan.getOriginTupleIds();
        if (originTupleIdList == null) {
            return;
        }
        exprUpdate(plan.getTupleIds(), originTupleIdList, resultExprList, context);
    }

    public static void exprUpdate(List<TupleId> newTupleIdList,
                                  List<TupleId> tupleIdList,
                                  List<? extends Expr> exprList,
                                  DecodeContext context) {
        if (exprList == null) {
            return;
        }
        List<TupleDescriptor> newTupleDescList = convertToTupleDescList(newTupleIdList, context);
        List<TupleDescriptor> tupleDescriptorList =  convertToTupleDescList(tupleIdList, context);
        for (Expr expr : exprList) {
            List<SlotRef> slotRefList = new ArrayList<>();
            SlotRef.getAllSlotRefFromExpr(expr, slotRefList);
            for (SlotRef slotRef : slotRefList) {
                SlotDescriptor slotDesc = slotRef.getDesc();
                TupleDescriptor tupleDescriptor = slotDesc.getParent();
                int index = tupleDescriptorList.indexOf(tupleDescriptor);
                if (index > -1) {
                    TupleDescriptor newTupleDesc = newTupleDescList.get(index);
                    slotRef.setDesc(newTupleDesc.getSlots().get(slotDesc.getSlotOffset()));
                }
            }
        }
    }

    private static  List<TupleDescriptor> convertToTupleDescList(List<TupleId> tupleIdList,
                                                                 DecodeContext context) {
        return tupleIdList
            .stream()
            .map(id -> context.getTableDesc().getTupleDesc(id))
            .collect(Collectors.toList());
    }


    private PlanNode insertDecodeNode(PlanNode parent, PlanNode plan) {
        Map<PlanNode, DecodeNode> childToDecodeMap = context.getChildToDecodeNode();
        DecodeNode decodeNode = childToDecodeMap.get(plan);

        if (parent != null && decodeNode != null) {
            List<PlanNode> children = parent.getChildren();
            for (int idx = 0; idx < children.size(); idx++) {
                PlanNode cur = children.get(idx);
                if (!cur.id.equals(plan.id)) {
                    continue;
                }
                children.set(idx, decodeNode);
                break;
            }
        }

        List<PlanNode> children = plan.getChildren();
        for (PlanNode child : children) {
            insertDecodeNode(plan, child);
        }
        if (parent == null) {
            if (decodeNode == null) {
                return plan;
            }
            return decodeNode;
        }
        return parent;
    }

    private void generateDecodeNode(PlanNode parent, PlanNode plan) {
        plan.generateDecodeNode(context);
        for (PlanNode child: plan.getChildren()) {
            generateDecodeNode(plan, child);
        }
    }

    private void updateNodes(PlanNode plan) {
        for (PlanNode child: plan.getChildren()) {
            updateNodes(child);
        }
        plan.updateSlots(context);
    }

    private void filterSupportedDictSlot(PlanNode plan) {
        plan.filterDictSlot(context);
        for (PlanNode child: plan.getChildren()) {
           filterSupportedDictSlot(child);
        }
    }

    private void findDictCodableSlot(PlanNode node) {

        if (node instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) node;
            TupleDescriptor tupleDesc = olapScanNode.getTupleDesc();
            OlapTable olapTable = olapScanNode.getOlapTable();
            long tableId = olapTable.getId();
            List<SlotDescriptor> slotsList = tupleDesc.getSlots();
            TupleDescriptor desc = olapScanNode.getTupleDesc();
            String dbName = desc.getRef().getName().getDb();
            Optional<Database> dbOp = Catalog.getCurrentCatalog().getDb(dbName);
            Preconditions.checkArgument(dbOp.isPresent());
            for (SlotDescriptor slotDesc : slotsList) {
                Column column = slotDesc.getColumn();
                String colName = column.getName();
                GlobalDictManger globalDictManger = Catalog.getServingCatalog().getGlobalDictMgr();
                IDict columnDict = globalDictManger.getDictForQuery(dbOp.get().getId(), tableId, colName);
                if (columnDict == null) {
                    continue;
                }
                int slotId = slotDesc.getId().asInt();
                context.addAvailableDict(slotId, columnDict);
            }
        }

        for (PlanNode planNode : node.getChildren()) {
            findDictCodableSlot(planNode);
        }
    }


}
