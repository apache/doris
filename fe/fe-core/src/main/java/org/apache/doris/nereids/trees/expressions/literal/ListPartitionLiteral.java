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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;

/** ListPartitionLiteral */
public class ListPartitionLiteral extends PartitionLiteral {

    private final List<Literal> values;

    public ListPartitionLiteral(Column partitionColumn,
            Slot partitionSlot, ListPartitionItem partitionItem, int partitionColumnIndex) {
        super(partitionColumn, partitionSlot, partitionItem, partitionColumnIndex);
        this.values = partitionItem.getItems().stream()
                .map(item -> Literal.fromLegacyLiteral(item.getKeys().get(partitionColumnIndex)))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public boolean enumerable() {
        return true;
    }

    @Override
    public long valueCount() {
        return values.size();
    }

    @Override
    public Iterator<? extends Literal> expendLiterals() {
        return values.iterator();
    }
}
