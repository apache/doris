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

package org.apache.doris.analysis;

import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

/** FILE constant wire carrier, independent of STRUCT. */
public final class FileLiteral extends LiteralExpr {
    // Used by Gson so Expr's transient fields are initialized during image replay.
    private FileLiteral() {
        type = Type.FILE;
        setNullableFromNereids(false);
    }

    public FileLiteral(List<LiteralExpr> fields) {
        Preconditions.checkArgument(fields.size() == 6, "FILE literal requires six children");
        type = Type.FILE;
        children = new ArrayList<>(fields);
        List<StructField> fileFields = Type.FILE.getFields();
        for (int i = 0; i < fields.size(); i++) {
            // Preserve canonical VARCHAR lengths and typed nullable children in the wire schema.
            children.set(i, fields.get(i).clone());
            children.get(i).setType(fileFields.get(i).getType());
        }
        setNullableFromNereids(false);
    }

    private FileLiteral(FileLiteral other) {
        super(other);
    }

    @Override
    public Expr clone() {
        return new FileLiteral(this);
    }

    @Override
    public String getStringValue() {
        JsonObject json = new JsonObject();
        List<StructField> fileFields = Type.FILE.getFields();
        for (int i = 0; i < children.size(); i++) {
            String name = fileFields.get(i).getName();
            if (children.get(i) instanceof NullLiteral) {
                json.add(name, JsonNull.INSTANCE);
            } else if (i == 1 || i == 2) {
                json.addProperty(name, ((LiteralExpr) children.get(i)).getLongValue());
            } else if (i == 5) {
                json.addProperty(name, Base64.getEncoder().encodeToString(
                        ((VarBinaryLiteral) children.get(i)).getValue()));
            } else {
                json.addProperty(name, children.get(i).getStringValue());
            }
        }
        return json.toString();
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr other) {
        throw new UnsupportedOperationException("FILE values cannot be compared");
    }

    @Override
    protected String toSqlImpl() {
        return toSqlImpl(false, false, null, null);
    }

    @Override
    protected String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table) {
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < children.size(); i++) {
            fields.add("'" + Type.FILE.getFields().get(i).getName() + "'");
            fields.add(children.get(i).toSqlImpl(disableTableName, needExternalSql, tableType, table));
        }
        return "CAST(named_struct(" + String.join(", ", fields) + ") AS FILE)";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.FILE_LITERAL;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof FileLiteral && children.equals(((FileLiteral) other).children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, children);
    }
}
