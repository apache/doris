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

import java.util.ArrayList;
import java.util.List;

public class PredicateUtils {
    /**
     * Split predicates in disjunctive form recursively, i.e., split the input expression
     * if the root node of the expression tree is `or` predicate.
     *
     * Some examples:
     * a or b -> a, b
     * a or b or c -> a, b, c
     * (a and b) or (c or d) -> (a and b), c, d
     * (a or b) and c -> (a or b) and c
     * a -> a
     */
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
