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

package org.apache.doris.indexpolicy;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class IndexPolicy implements Writable, GsonPostProcessable {

    public static final ShowResultSetMetaData INDEX_POLICY_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("id", ScalarType.createVarchar(20)))
                    .addColumn(new Column("name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("type", ScalarType.createVarchar(20)))
                    .addColumn(new Column("properties", ScalarType.createVarchar(100)))
                    .build();

    public static final String PROP_TYPE = "type";
    public static final String PROP_ANALYZER = "analyzer";
    public static final String PROP_TOKENIZER = "tokenizer";
    public static final String PROP_TOKEN_FILTER = "token_filter";

    public static final Set<String> BUILTIN_TOKENIZERS = ImmutableSet.of(
            "ngram", "edge_ngram", "keyword", "standard", "char_group");

    public static final Set<String> BUILTIN_TOKEN_FILTERS = ImmutableSet.of(
            "asciifolding", "word_delimiter", "lowercase");

    private static final Logger LOG = LogManager.getLogger(IndexPolicy.class);

    @SerializedName(value = "id")
    private long id;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "type")
    protected IndexPolicyTypeEnum type;

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public IndexPolicy(long id, String name, IndexPolicyTypeEnum type, Map<String, String> properties) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.properties = properties;
    }

    public static IndexPolicy create(String name, IndexPolicyTypeEnum type,
            Map<String, String> properties) throws DdlException {
        long id = Env.getCurrentEnv().getNextId();
        IndexPolicy indexPolicy = new IndexPolicy(id, name, type, properties);
        return indexPolicy;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static IndexPolicy read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, IndexPolicy.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {}

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    public List<String> getShowInfo() {
        return Lists.newArrayList(String.valueOf(this.id), this.name, this.type.toString(),
                GsonUtils.GSON.toJson(this.properties));
    }

    public boolean isInvalid() {
        return false;
    }
}
