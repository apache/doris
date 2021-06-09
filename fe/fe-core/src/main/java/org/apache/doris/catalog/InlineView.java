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

package org.apache.doris.catalog;

import org.apache.doris.thrift.TTableDescriptor;

import java.util.List;

/**
 * A fake catalog representation of an inline view. It's like a table. It has name
 * and columns, but it won't have ids and it shouldn't be converted to Thrift.
 * An inline view is constructed by providing an alias (required) and then adding columns
 * one-by-one.
 */

public class InlineView extends Table {
    /**
     * An inline view only has an alias and columns, but it won't have id, db and owner.
     */
    public InlineView(String alias, List<Column> columns) {
        super(-1, alias, TableType.INLINE_VIEW, columns);
        // ID for inline view has no use.
    }

    /**
     * An inline view only has an alias and columns, but it won't have id, db and owner.
     */
    public InlineView(View view, List<Column> columns) {
        // TODO(zc): think about it
        super(-1, view.getName(), TableType.INLINE_VIEW, columns);
    }

    /**
     * This should never be called.
     */
    public TTableDescriptor toThrift() {
        // An inline view never generate Thrift representation.
        throw new UnsupportedOperationException("Inline View should not generate Thrift representation");
    }

    public int getNumNodes() {
        throw new UnsupportedOperationException("InlineView.getNumNodes() not supported");
    }

}
