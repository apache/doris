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

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TJsonLiteral;

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;
import java.util.Objects;

public class JsonLiteral extends LiteralExpr {
    private JsonParser parser = new JsonParser();
    @SerializedName("v")
    private String value;
    // Means the converted session variable need to be cast to int, such as "cast 'STRICT_TRANS_TABLES' to Integer".
    private String beConverted = "";

    public JsonLiteral() {
        super();
        type = Type.JSONB;
    }

    public JsonLiteral(String value) throws AnalysisException {
        try {
            parser.parse(value);
        } catch (JsonSyntaxException e) {
            throw new AnalysisException("Invalid jsonb literal: " + e.getMessage());
        }
        this.value = value;
        type = Type.JSONB;
        analysisDone();
    }

    protected JsonLiteral(JsonLiteral other) {
        super(other);
        value = other.value;
    }

    public void setBeConverted(String val) {
        this.beConverted = val;
    }

    @Override
    public Expr clone() {
        return new JsonLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        throw new RuntimeException("Not support comparison between JSONB literals");
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public String toSqlImpl() {
        return "'" + value.replaceAll("'", "''") + "'";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.JSON_LITERAL;
        msg.json_literal = new TJsonLiteral(getUnescapedValue());
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public String getStringValueForArray(FormatOptions options) {
        return null;
    }

    public String getUnescapedValue() {
        // Unescape string exactly like Hive does. Hive's method assumes
        // quotes so we add them here to reuse Hive's code.
        return value;
    }

    public String getJsonValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        throw new RuntimeException("JSONB value cannot be parsed as Long value");
    }

    @Override
    public double getDoubleValue() {
        throw new RuntimeException("JSONB value cannot be parsed as Double value");
    }

    @Override
    public String getRealValue() {
        return getJsonValue();
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        // code should not be readched, since JSONB is analyzed as StringLiteral
        throw new AnalysisException("Unknown check type: " + targetType);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = Text.readString(in);
    }

    public static JsonLiteral read(DataInput in) throws IOException {
        JsonLiteral literal = new JsonLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
