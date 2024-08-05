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

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import java.util.ArrayList;
import java.util.List;

/**
 * mapping the real slot to virtual slots, grouping(_id) function will use a virtual slot of BIGINT to substitute
 * real slots, and then set real slot to realChildren
 */
public class GroupingFunctionCallExpr extends FunctionCallExpr {
    private boolean childrenReseted = false;
    private List<Expr> realChildren;

    public GroupingFunctionCallExpr(String functionName, List<Expr> params) {
        super(functionName, params);
        childrenReseted = false;
    }

    public GroupingFunctionCallExpr(FunctionName functionName, FunctionParams params) {
        super(functionName, params);
        childrenReseted = false;
    }

    public GroupingFunctionCallExpr(GroupingFunctionCallExpr other) {
        super(other);
        this.childrenReseted = other.childrenReseted;
        if (this.childrenReseted) {
            this.realChildren = Expr.cloneList(other.realChildren);
        }
    }

    @Override
    public Expr clone() {
        return new GroupingFunctionCallExpr(this);
    }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        if (children.size() < 1) {
            throw new AnalysisException("GROUPING functions required at least one parameters");
        }
        for (Expr expr : children) {
            if (expr instanceof SlotRef) {
                continue;
            } else {
               // throw new AnalysisException("GROUPING functions required columns as parameters");
            }
        }
        Type[] childTypes = new Type[1];
        childTypes[0] = Type.BIGINT;
        fn = getBuiltinFunction(getFnName().getFunction(), childTypes, Function.CompareMode.IS_IDENTICAL);
        this.type = fn.getReturnType();
    }

    // set child to virtual slot
    public void resetChild(VirtualSlotRef virtualSlot) {
        ArrayList<Expr> newChildren = new ArrayList<>();
        newChildren.add(virtualSlot);
        realChildren = new ArrayList<>();
        realChildren.addAll(children);
        children = newChildren;
        childrenReseted = true;
    }

    @Override
    public Expr reset() {
        if (childrenReseted) {
            children = new ArrayList<>();
            children.addAll(realChildren);
        }
        childrenReseted = false;
        realChildren = null;
        return super.reset();
    }

    // get the origin children of the expr
    public List<Expr> getRealSlot() {
        if (childrenReseted) {
            List<Expr> result = new ArrayList<>();
            for (Expr expr : realChildren) {
                result.add(expr);
            }
            return result;
        } else if (isAnalyzed()) {
            List<Expr> result = new ArrayList<>();
            for (Expr expr : children) {
                result.add(expr);
            }
            return result;
        } else {
            return null;
        }
    }

    public List<Expr> getRealChildren() {
        return realChildren;
    }

    @Override
    public boolean supportSerializable() {
        return false;
    }
}
