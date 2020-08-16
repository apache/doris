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

import java.util.List;

/**
 * Return value of the grammar production that parses aggregate function
 * parameters.
 */
class AggregateParamsList {
    private final boolean    isStar;
    private final boolean    isDistinct;
    private       List<Expr> exprs;

    // c'tor for non-star params
    public AggregateParamsList(boolean isDistinct, List<Expr> exprs) {
        super();
        isStar = false;
        this.isDistinct = isDistinct;
        this.exprs = exprs;
    }

    // c'tor for <agg>(*)
    private AggregateParamsList() {
        super();
        isStar = true;
        isDistinct = false;
    }

    static public AggregateParamsList createStarParam() {
        return new AggregateParamsList();
    }

    public boolean isStar() {
        return isStar;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public List<Expr> exprs() {
        return exprs;
    }
}
