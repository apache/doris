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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.Utils;

import com.alibaba.google.common.collect.Lists;

import java.util.List;

/**
 * Slot has not been bound.
 */
public class UnboundSlot extends Slot {
    private final List<String> nameParts;

    public UnboundSlot(List<String> nameParts) {
        super(NodeType.UNBOUND_SLOT);
        this.nameParts = nameParts;
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    @Override
    public String getName() {
        return nameParts.stream().map(n -> {
            if (n.contains(".")) {
                return "`" + n + "`";
            } else {
                return n;
            }
        }).reduce((left, right) -> left + "." + right).orElse("");
    }

    @Override
    public String sql() {
        return nameParts.stream().map(Utils::quoteIfNeeded).reduce((left, right) -> left + "." + right).orElse("");
    }

    public static UnboundSlot quoted(String name) {
        return new UnboundSlot(Lists.newArrayList(name));
    }

    @Override
    public String toString() {
        return "'" + getName();
    }
}
