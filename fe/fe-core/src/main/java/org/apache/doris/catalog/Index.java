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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TIndexType;
import org.apache.doris.thrift.TOlapTableIndex;

import com.google.common.io.ByteStreams;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Internal representation of index, including index type, name, columns and comments.
 * This class will used in olaptable
 */
public class Index implements Writable {
    @SerializedName(value = "indexName")
    private String indexName;
    @SerializedName(value = "columns")
    private List<String> columns;
    @SerializedName(value = "indexType")
    private IndexDef.IndexType indexType;
    @SerializedName(value = "arguments")
    @JsonAdapter(ExprListGsonAdapter.class)
    private List<Expr> arguments;
    @SerializedName(value = "comment")
    private String comment;

    public Index(String indexName, List<String> columns, IndexDef.IndexType indexType,
                 List<Expr> arguments, String comment) {
        this.indexName = indexName;
        this.columns = columns;
        this.indexType = indexType;
        this.arguments = arguments;
        this.comment = comment;
    }

    public Index(String indexName, List<String> columns, IndexDef.IndexType indexType, String comment) {
        this(indexName, columns, indexType, new ArrayList<Expr>(), comment);
    }

    public Index() {
        this.indexName = null;
        this.columns = null;
        this.indexType = null;
        this.arguments = null;
        this.comment = null;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public IndexDef.IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexDef.IndexType indexType) {
        this.indexType = indexType;
    }

    public List<Expr> getArguments() {
        return arguments;
    }

    public void setArguments(List<Expr> arguments) {
        this.arguments = arguments;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Index read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Index.class);
    }

    @Override
    public int hashCode() {
        return 31 * (indexName.hashCode() + columns.hashCode() + indexType.hashCode() + Objects.hashCode(arguments));
    }

    public Index clone() {
        return new Index(indexName, new ArrayList<>(columns), indexType,
            arguments == null ? null : new ArrayList<>(arguments), comment);
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(indexName);
        sb.append(" (");
        boolean first = true;
        for (String col : columns) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("`" + col + "`");
        }
        sb.append(")");
        if (indexType != null) {
            sb.append(" USING ").append(indexType.toString());
        }
        if (arguments != null && !arguments.isEmpty()) {
            sb.append(" (");
            first = true;
            for (Expr argument : arguments) {
                if (first) {
                    first = false;
                } else {
                    sb.append(",");
                }
                sb.append(argument.toSql());
            }
            sb.append(")");
        }
        if (comment != null) {
            sb.append(" COMMENT '" + comment + "'");
        }
        return sb.toString();
    }

    public TOlapTableIndex toThrift() {
        TOlapTableIndex tIndex = new TOlapTableIndex();
        tIndex.setIndexName(indexName);
        tIndex.setColumns(columns);
        tIndex.setIndexType(TIndexType.valueOf(indexType.toString()));
        if (columns != null) {
            tIndex.setComment(comment);
        }
        if (arguments != null) {
            List<TExpr> tArguments = new ArrayList<>(arguments.size());
            for (Expr argument : arguments) {
                TExpr tArgument = argument.treeToThrift();
                tArguments.add(tArgument);
            }
            tIndex.setArguments(tArguments);
        }
        return tIndex;
    }

    public static class ExprListGsonAdapter extends TypeAdapter<List<Expr>> {
        @Override
        public void write(JsonWriter jsonWriter, List<Expr> exprs) throws IOException {
            if (exprs == null) {
                jsonWriter.nullValue();
            } else {
                ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
                DataOutput output = ByteStreams.newDataOutput(byteArray);
                output.writeInt(exprs.size());
                for (Expr expr : exprs) {
                    Expr.writeTo(expr, output);
                }
                byte[] bytes = byteArray.toByteArray();
                String base64 = Base64.encodeBase64String(bytes);
                jsonWriter.value(base64);
            }
        }

        @Override
        public List<Expr> read(JsonReader jsonReader) throws IOException {
            if (jsonReader.hasNext()) {
                List<Expr> exprs = new ArrayList<>();
                String base64 = jsonReader.nextString();
                byte[] bytes = Base64.decodeBase64(base64);
                DataInput in = ByteStreams.newDataInput(bytes);
                int count = in.readInt();
                for (int i = 0; i < count; i++) {
                    exprs.add(Expr.readIn(in));
                }
                return exprs;
            } else {
                return null;
            }
        }
    }

    public static void checkConflict(Collection<Index> indices, Set<String> bloomFilters) throws AnalysisException {
        indices = indices == null ? Collections.emptyList() : indices;
        bloomFilters = bloomFilters == null ? Collections.emptySet() : bloomFilters;
        Set<String> bfColumns = new HashSet<>();
        for (Index index : indices) {
            if (IndexDef.IndexType.NGRAM_BF == index.indexType) {
                for (String column : index.getColumns()) {
                    column = column.toLowerCase();
                    if (bfColumns.contains(column)) {
                        throw new AnalysisException(column + " already has ngram bloom filter index");
                    }
                    bfColumns.add(column);
                }
            }
        }
        for (String column : bloomFilters) {
            column = column.toLowerCase();
            if (bfColumns.contains(column)) {
                throw new AnalysisException(column
                                            + " should have only one ngram bloom filter index or bloom filter index");
            }
            bfColumns.add(column);
        }
    }
}
