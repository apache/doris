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

package org.apache.doris.planner.external.paimon;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.external.PaimonExternalTable;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.planner.ColumnRange;
import org.apache.doris.thrift.TFileAttributes;

import org.apache.paimon.table.Table;

import java.util.Map;

public class PaimonSource {
    private final PaimonExternalTable paimonExtTable;
    private final Table originTable;

    private final TupleDescriptor desc;

    public PaimonSource(PaimonExternalTable table, TupleDescriptor desc,
                            Map<String, ColumnRange> columnNameToRange) {
        this.paimonExtTable = table;
        this.originTable = paimonExtTable.getOriginTable();
        this.desc = desc;
    }

    public TupleDescriptor getDesc() {
        return desc;
    }

    public Table getPaimonTable() {
        return originTable;
    }

    public TableIf getTargetTable() {
        return paimonExtTable;
    }

    public TFileAttributes getFileAttributes() throws UserException {
        return new TFileAttributes();
    }

    public ExternalCatalog getCatalog() {
        return paimonExtTable.getCatalog();
    }
}
