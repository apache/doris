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
import org.apache.doris.optimizer.base.OptLogicalProperty;
import org.apache.doris.optimizer.base.OptProperty;
import org.apache.doris.optimizer.stat.Statistics;
import org.apache.doris.optimizer.stat.StatisticsContext;

import java.util.BitSet;

public abstract class OptLogical extends OptOperator {

    protected OptLogical(OptOperatorType type) {
        super(type);
    }

    public abstract BitSet getCandidateRulesForExplore();

    public abstract BitSet getCandidateRulesForImplement();

    public abstract Statistics deriveStat(OptExpressionWapper wapper, StatisticsContext context);

    @Override
    public boolean isLogical() { return true; }

    @Override
    public OptProperty createProperty() {
        return new OptLogicalProperty();
    }

    // TODO(zc): returning null to make compiler happy
    public OptColumnRefSet getOutputColumns(OptExpression expression) {
        return null;
    }
}
