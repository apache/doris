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

package org.apache.doris.nereids.spm.matcher;

import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Query-local bindings with checkpoints for tentative unordered matches. */
public final class PlaceholderBindings extends AbstractMap<Long, Expression> {
    private final Map<Long, Expression> values;
    private final List<Change> changes = new ArrayList<>();

    private record Change(Long id, boolean present, Expression previous) {
    }

    private PlaceholderBindings(Map<Long, Expression> values) {
        this.values = values;
    }

    /** Reuses an enclosing journal when a subquery or expression check is nested. */
    public static PlaceholderBindings track(Map<Long, Expression> values) {
        return values instanceof PlaceholderBindings
                ? (PlaceholderBindings) values : new PlaceholderBindings(values);
    }

    public int checkpoint() {
        return changes.size();
    }

    /** Restores only writes made after the checkpoint. */
    public void rollback(int checkpoint) {
        for (int i = changes.size() - 1; i >= checkpoint; i--) {
            Change change = changes.remove(i);
            if (change.present()) {
                values.put(change.id(), change.previous());
            } else {
                values.remove(change.id());
            }
        }
    }

    @Override
    public Expression put(Long id, Expression value) {
        boolean present = values.containsKey(id);
        Expression previous = values.put(id, value);
        changes.add(new Change(id, present, previous));
        return previous;
    }

    @Override
    public Expression get(Object id) {
        return values.get(id);
    }

    @Override
    public boolean containsKey(Object id) {
        return values.containsKey(id);
    }

    @Override
    public int size() {
        return values.size();
    }

    @Override
    public void clear() {
        values.clear();
        changes.clear();
    }

    @Override
    public Set<Entry<Long, Expression>> entrySet() {
        return values.entrySet();
    }
}
