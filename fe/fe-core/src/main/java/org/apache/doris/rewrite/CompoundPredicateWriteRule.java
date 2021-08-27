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
 * add Rewrite CompoundPredicates 'OR' 'AND' 'NOT' Rule
 * It can be applied to pre-analysis expr trees and therefore does not reanalyze
 * the transformation output itself.
 * Examples:
 * OR:
 * (-2==2 OR city_id=2) ==> city_id=2
 * (city_id=2 OR -2==2) ==> city_id=2
 * -5!=-5 OR citycode=0 ==> citycode=0
 * AND:
 * (citycode=0 AND 1=1) ==> citycode=0
 * -5=-5 AND citycode=0 AND 2=2 ==> citycode=0
 */

public class CompoundPredicateWriteRule implements ExprRewriteRule {
    public static ExprRewriteRule INSTANCE = new CompoundPredicateWriteRule();

    @Override
    public Expr apply(Expr expr, Analyzer analyzer) throws AnalysisException {

        if (!(expr instanceof CompoundPredicate)) return expr;
        CompoundPredicate cp = (CompoundPredicate) expr;
        Expr result = expr;

        List<Expr> children = cp.getChildren();
        if (children.size() != 2) {
            return expr;
        }
        //rewrite OR
        if (cp.getOp() == CompoundPredicate.Operator.OR) {

            Expr leftChild = cp.getChild(0);
            Expr rightChild = cp.getChild(1);

            //OR leftChild is bool
            // true OR expr ==> true
            // false OR expr ==> expr
            if (leftChild instanceof BoolLiteral) {
                BoolLiteral boolLiteralLeftChild = (BoolLiteral) leftChild;

                if (boolLiteralLeftChild.getValue()) {
                    return new BoolLiteral(true);
                } else {
                    result = rightChild;
                }
            }

            //OR rightChild is bool
            // expr OR true ==> true
            // expr OR false ==> expr
            if (rightChild instanceof BoolLiteral) {
                BoolLiteral boolLiteralRightChild = (BoolLiteral) rightChild;

                if (boolLiteralRightChild.getValue()) {
                    return new BoolLiteral(true);
                } else {
                    result = leftChild;
                }
            }

            return result;
        }

        //rewrite AND
        if (cp.getOp() == CompoundPredicate.Operator.AND) {

            Expr leftChild = cp.getChild(0);
            Expr rightChild = cp.getChild(1);

            //AND leftChild is bool
            // true AND expr ==> expr
            // false AND expr ==> false
            if (leftChild instanceof BoolLiteral) {
                BoolLiteral boolLiteralLeftChild = (BoolLiteral) leftChild;

                if (boolLiteralLeftChild.getValue()) {
                    result = rightChild;
                } else {
                    return new BoolLiteral(false);
                }
            }

            //AND rightChild is bool
            // expr AND true ==> expr
            // expr AND false ==> false
            if (rightChild instanceof BoolLiteral) {
                BoolLiteral boolLiteralRightChild = (BoolLiteral) rightChild;

                if (boolLiteralRightChild.getValue()) {
                    result = leftChild;
                } else {
                    return new BoolLiteral(false);
                }
            }
            return result;
        }

        return result;
    }
}
