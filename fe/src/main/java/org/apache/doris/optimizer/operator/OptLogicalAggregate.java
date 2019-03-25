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

package org.apache.doris.optimizer.operator;

import org.apache.doris.optimizer.OptExpression;
import org.apache.doris.optimizer.OptExpressionWapper;
import org.apache.doris.optimizer.base.OptColumnRefSet;
import org.apache.doris.optimizer.rule.OptRule;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsContext;

import java.util.BitSet;
import java.util.List;

public class OptLogicalAggregate extends OptLogical {

    public OptLogicalAggregate() {
        super(OptOperatorType.OP_LOGICAL_AGGREGATE);
    }

    @Override
    public BitSet getCandidateRulesForExplore() {
        return null;
    }

    @Override
    public BitSet getCandidateRulesForImplement() {
        return null;
    }

    @Override
    public OptColumnRefSet getOutputColumns(OptExpression expression) {
        return expression.getLogicalProperty().getOutputColumns();
    }

    @Override
    public Statistics deriveStat(OptExpressionWapper wapper, StatisticsContext context) {
        return null;
    }


}
