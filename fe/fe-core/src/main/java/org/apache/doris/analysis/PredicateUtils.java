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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;

public class PredicateUtils {

    public static List<Expr> splitDisjunctivePredicates(Expr expr) {
        ArrayList<Expr> result = Lists.newArrayList();
        if (expr == null) {
            return result;
        }

        splitDisjunctivePredicates(expr, result);
        return result;
    }

    private static void splitDisjunctivePredicates(Expr expr, List<Expr> result) {
        if (expr instanceof CompoundPredicate && ((CompoundPredicate) expr).getOp() == CompoundPredicate.Operator.OR) {
            splitDisjunctivePredicates(expr.getChild(0), result);
            splitDisjunctivePredicates(expr.getChild(1), result);
        } else {
            result.add(expr);
        }
    }
}
