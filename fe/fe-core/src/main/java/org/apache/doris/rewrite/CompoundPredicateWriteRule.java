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

package org.apache.doris.rewrite;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.common.AnalysisException;

import java.util.List;

/**
 *  Add rewrite CompoundPredicates 'OR' 'AND' rule
 *  'OR' 'AND' rewrite rule
 *  case true and expr ==> expr
 *  case expr and true ==> expr
 *  case false or expr ==> expr
 *  case expr or false ==> expr
 *
 *  case false and expr ==> false
 *  case expr and false ==> false
 *  case true or expr ==> true
 *  case expr or true ==> true
 */

public class CompoundPredicateWriteRule implements ExprRewriteRule {

    public static ExprRewriteRule INSTANCE = new CompoundPredicateWriteRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer, ExprRewriter.ClauseType clauseType) throws AnalysisException {

        if (!(expr instanceof CompoundPredicate)) {
            return expr;
        }
        CompoundPredicate cp = (CompoundPredicate) expr;

        List<Expr> children = cp.getChildren();
        if (children.size() != 2) {
            return expr;
        }
        Expr leftChild = cp.getChild(0);
        Expr rightChild = cp.getChild(1);

        boolean and = (cp.getOp() == CompoundPredicate.Operator.AND);
        boolean or = (cp.getOp() == CompoundPredicate.Operator.OR);

        boolean leftChildTrue = (leftChild instanceof BoolLiteral) && (((BoolLiteral) leftChild).getValue());
        boolean leftChildFalse = (leftChild instanceof BoolLiteral) && (!((BoolLiteral) leftChild).getValue());

        boolean rightChildTrue = (rightChild instanceof BoolLiteral) && (((BoolLiteral) rightChild).getValue());
        boolean rightChildFalse = (rightChild instanceof BoolLiteral) && (!((BoolLiteral) rightChild).getValue());

        // case true and expr ==> expr
        if (leftChildTrue && and) {
            return rightChild;
        }
        // case expr and true ==> expr
        if (and && rightChildTrue) {
            return leftChild;
        }
        // case false or expr ==> expr
        if (leftChildFalse && or) {
            return rightChild;
        }
        // case expr or false ==> expr
        if (or && rightChildFalse) {
            return leftChild;
        }

        // case false and expr ==> false
        if (leftChildFalse && and) {
            return new BoolLiteral(false);
        }
        // case expr and false ==> false
        if (and && rightChildFalse) {
            return new BoolLiteral(false);
        }
        // case true or expr ==> true
        if (leftChildTrue && or) {
            return new BoolLiteral(true);
        }
        // case expr or true ==> true
        if (or && rightChildTrue) {
            return new BoolLiteral(true);
        }

        // other case ,return origin expr
        return expr;
    }
}
