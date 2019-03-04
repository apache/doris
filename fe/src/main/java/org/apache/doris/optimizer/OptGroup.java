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

package org.apache.doris.optimizer;

import java.util.List;

// A Group contains all logical equivalent logical MultiExpressions
// and physical MultiExpressions
public class OptGroup {
    private int id;
    private List<MultiExpression> mExprs;
    private int nextMExprId = 1;

    public OptGroup(int id) {
        this.id = id;
    }

    public int getId() { return id; }

    // Add a MultiExpression
    public void addMExpr(MultiExpression mExpr) {
        mExpr.setId(nextMExprId++);
        mExprs.add(mExpr);
    }
}
