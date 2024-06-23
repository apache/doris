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

import org.apache.doris.common.FeNameFormat;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.thrift.TTableDescriptor;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * A fake catalog representation of an inline view. It's like a table. It has name
 * and columns, but it won't have ids and it shouldn't be converted to Thrift.
 * An inline view is constructed by providing an alias (required) and then adding columns
 * one-by-one.
 */

public class InlineView extends Table {

    protected Map<String, Column> innerNameToColumn;

    // Some catalog has its own specific view grammar。for example: inner name like _c1,_c2 in hive
    protected InitCatalogLog.Type catalogType;

    /**
     * An inline view only has an alias and columns, but it won't have id, db and owner.
     */
    public InlineView(String alias, List<Column> columns, InitCatalogLog.Type catalogType) {
        // ID for inline view has no use.
        super(-1, alias, TableType.INLINE_VIEW, columns);
        this.catalogType = catalogType;
        init();
    }

    /**
     * An inline view only has an alias and columns, but it won't have id, db and owner.
     */
    public InlineView(View view, List<Column> columns, InitCatalogLog.Type catalogType) {
        // TODO(zc): think about it
        super(-1, view.getName(), TableType.INLINE_VIEW, columns);
        this.catalogType = catalogType;
        init();
    }

    private void init() {
        if (this.catalogType == InitCatalogLog.Type.HMS) {
            initColumnInnerNameMap();
        }
    }

    /**
     * Support get column from inline view with column name like '_c0', '_c1' etc for hive.
     */
    public void initColumnInnerNameMap() {
        innerNameToColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        if (this.fullSchema != null) {
            int counter = -1;
            for (Column col : this.fullSchema) {
                counter++;
                // only set inner name for derived column
                if (!col.getName().matches(FeNameFormat.getColumnNameRegex())) {
                    innerNameToColumn.put("_c" + counter, col);
                }
            }
        }
    }

    public Column getColumn(String name) {
        Column column = nameToColumn.get(name);

        if (column == null && this.catalogType == InitCatalogLog.Type.HMS) {
            column = innerNameToColumn.get(name);
        }

        return column;
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
