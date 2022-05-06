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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.nereids.trees.NodeType;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.util.Utils;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Star expression.
 */
public class UnboundStar extends NamedExpression {
    private final List<String> target;

    public UnboundStar(List<String> target) {
        super(NodeType.UNBOUND_STAR);
        this.target = target;
    }

    @Override
    public String sql() {
        String targetString = target.stream().map(Utils::quoteIfNeeded).reduce((t1, t2) -> t1 + "." + t2).orElse("");
        if (StringUtils.isNotEmpty(targetString)) {
            return targetString + ".*";
        } else {
            return "*";
        }
    }

    @Override
    public String toString() {
        return sql();
    }
}
