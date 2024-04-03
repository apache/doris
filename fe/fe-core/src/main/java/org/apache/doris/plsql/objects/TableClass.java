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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/objects/TableClass.java
// and modified by Doris

package org.apache.doris.plsql.objects;

import org.apache.doris.plsql.ColumnDefinition;
import org.apache.doris.plsql.Row;
import org.apache.doris.plsql.Var;
import org.apache.doris.plsql.objects.MethodParams.Arity;

import java.util.List;


public class TableClass implements PlClass {
    private static final MethodDictionary<Table> methodDictionary = new MethodDictionary<>();
    private final String typeName;
    private final List<ColumnDefinition> columns;
    private final boolean rowType;

    static {
        methodDictionary.put("first", (self, args) -> {
            Arity.NULLARY.check("first", args);
            return wrap(self.firstKey());
        });
        methodDictionary.put("last", (self, args) -> {
            Arity.NULLARY.check("last", args);
            return wrap(self.lastKey());
        });
        methodDictionary.put("next", (self, args) -> {
            Long key = new MethodParams("next", args, Arity.UNARY).longAt(0);
            return wrap(self.nextKey(key));
        });
        methodDictionary.put("prior", (self, args) -> {
            Long key = new MethodParams("prior", args, Arity.UNARY).longAt(0);
            return wrap(self.priorKey(key));
        });
        methodDictionary.put("count", (self, args) -> {
            Arity.NULLARY.check("count", args);
            return new Var(Long.valueOf(self.count()));
        });
        methodDictionary.put("exists", (self, args) -> {
            Long key = new MethodParams("exists", args, Arity.UNARY).longAt(0);
            return new Var(self.existsAt(key));
        });
        methodDictionary.put("delete", (self, args) -> {
            Arity.max(3).check("delete", args);
            if (args.isEmpty()) {
                self.removeAll();
            } else if (args.size() == 1) {
                self.removeAt(args.get(0).value);
            } else {
                self.removeFromTo(args.get(0).value, args.get(1).value);
            }
            return null;
        });
        methodDictionary.put(MethodDictionary.__GETITEM__, (self, args) -> {
            Long key = new MethodParams(MethodDictionary.__GETITEM__, args, Arity.UNARY).longAt(0);
            Row row = self.at(key);
            if (row == null) {
                return Var.Null;
            }
            if (self.plClass().rowType()) {
                Var var = new Var();
                var.setType(Var.Type.ROW.name());
                var.setValue(row);
                return var;
            }
            return row.getValue(0);
        });
        methodDictionary.put(MethodDictionary.__SETITEM__, (self, args) -> {
            MethodParams params = new MethodParams(MethodDictionary.__SETITEM__, args, Arity.BINARY);
            long key = params.longAt(0);
            if (self.plClass().rowType()) {
                self.put(key, params.rowAt(1));
            } else { // single column
                Row row = new Row();
                row.addColumn(
                        self.plClass().columns().get(0).columnName(),
                        self.plClass().columns.get(0).columnTypeString(),
                        args.get(1));
                self.put(key, row);
            }
            return Var.Null;
        });
    }

    private static Var wrap(Object result) {
        return result != null ? new Var((Long) result) : Var.Null;
    }

    public TableClass(String typeName, List<ColumnDefinition> columns, boolean rowType) {
        this.typeName = typeName;
        this.columns = columns;
        this.rowType = rowType;
    }

    public String typeName() {
        return typeName;
    }

    public List<ColumnDefinition> columns() {
        return columns;
    }

    @Override
    public Table newInstance() {
        return new Table(this);
    }

    @Override
    public MethodDictionary methodDictionary() {
        return methodDictionary;
    }

    public boolean rowType() {
        return rowType;
    }
}
