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

        List<Expr> children = cp.getChildren();
        if (children.size() != 2) {
            return expr;
        }

        /*
         * following 'OR' 'AND' rewrite rule refer to Spark code
         * Spark uses eight 'case' statements to make the code more streamlined
         *
         *  case true AND expr ==> expr
         *  case expr AND true ==> expr
         *  case false Or expr ==> expr
         *  case expr Or false ==> expr
         *
         *  case false AND expr ==> false
         *  case expr AND false ==> false
         *  case true Or expr ==> true
         *  case expr Or true ==> true
         *
         */
        Expr leftChild = cp.getChild(0);
        Expr rightChild = cp.getChild(1);

        boolean opAND = (cp.getOp() == CompoundPredicate.Operator.AND);
        boolean opOr = (cp.getOp() == CompoundPredicate.Operator.OR);

        boolean leftChildTrue = (leftChild instanceof BoolLiteral)&&(((BoolLiteral)leftChild).getValue());
        boolean leftChildFalse = (leftChild instanceof BoolLiteral)&&(!((BoolLiteral)leftChild).getValue());

        boolean rightChildTrue = (rightChild instanceof BoolLiteral)&&(((BoolLiteral)rightChild).getValue());
        boolean rightChildFalse = (rightChild instanceof BoolLiteral)&&(!((BoolLiteral)rightChild).getValue());

        //case true AND expr ==> expr
        if(leftChildTrue && opAND) return rightChild;
        //case expr AND true ==> expr
        if(opAND && rightChildTrue) return leftChild;
        //case false Or expr ==> expr
        if(leftChildFalse && opOr) return rightChild;
        //case expr Or false ==> expr
        if(opOr && rightChildFalse) return leftChild;

        //case false AND expr ==> false
        if(leftChildFalse && opAND) return new BoolLiteral(false);
        //case expr AND false ==> false
        if(opAND && rightChildFalse) return new BoolLiteral(false);
        //case true Or expr ==> true
        if(leftChildTrue && opOr) return new BoolLiteral(true);
        //case expr Or true ==> true
        if(opOr && rightChildTrue) return new BoolLiteral(true);

        //other case ,return origin expr
        return expr;
    }
}
