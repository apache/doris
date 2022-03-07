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

import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;

public class MVColumnBitmapUnionPattern implements MVColumnPattern {

    @Override
    public boolean match(Expr expr) {
        if (!(expr instanceof FunctionCallExpr)) {
            return false;
        }
        FunctionCallExpr fnExpr = (FunctionCallExpr) expr;
        if (fnExpr.isDistinct()) {
            return false;
        }
        String fnNameString = fnExpr.getFnName().getFunction();
        if (!fnNameString.equalsIgnoreCase(FunctionSet.BITMAP_UNION)) {
            return false;
        }
        if (fnExpr.getChild(0) instanceof SlotRef) {
            SlotRef slotRef = (SlotRef) fnExpr.getChild(0);
            if (slotRef.getType().getPrimitiveType() == PrimitiveType.BITMAP && slotRef.getColumn() != null) {
                return true;
            } else {
                return false;
            }
        } else if (fnExpr.getChild(0) instanceof FunctionCallExpr) {
            FunctionCallExpr child0FnExpr = (FunctionCallExpr) fnExpr.getChild(0);
            if (!child0FnExpr.getFnName().getFunction().equalsIgnoreCase(FunctionSet.TO_BITMAP)) {
                return false;
            }
            SlotRef slotRef = child0FnExpr.getChild(0).unwrapSlotRef();
            if (slotRef == null) {
                return false;
            } else if (slotRef.getType().isIntegerType()) {
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return FunctionSet.BITMAP_UNION + "(" + FunctionSet.TO_BITMAP + "(column)), type of column could not be integer. "
                + "Or " + FunctionSet.BITMAP_UNION + "(bitmap_column) in agg table";
    }
}
