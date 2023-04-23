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
import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TGeometryLiteral;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class GeometryLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(GeometryLiteral.class);
    private String value;
    // Means the converted session variable need to be cast to int, such as "cast 'STRICT_TRANS_TABLES' to Integer".
    private String beConverted = "";

    public GeometryLiteral() {
        super();
        type = Type.GEOMETRY;
    }

    public GeometryLiteral(String value) throws AnalysisException {
        super();
        this.value = value;
        type = Type.GEOMETRY;
        analysisDone();
    }

    protected GeometryLiteral(GeometryLiteral other) {
        super(other);
        value = other.value;
    }

    public void setBeConverted(String val) {
        this.beConverted = val;
    }

    @Override
    public Expr clone() {
        return new GeometryLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        throw new RuntimeException("Not support comparison between GEOMETRY literals");
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
        msg.node_type = TExprNodeType.GEOMETRY_LITERAL;
        msg.geometry_literal = new TGeometryLiteral(getUnescapedValue());
    }

    @Override
    public String getStringValue() {
        return value;
    }

    @Override
    public String getStringValueForArray() {
        return null;
    }

    public String getUnescapedValue() {
        // Unescape string exactly like Hive does. Hive's method assumes
        // quotes so we add them here to reuse Hive's code.
        return value;
    }

    public String getGeometryValue() {
        return value;
    }

    @Override
    public long getLongValue() {
        throw new RuntimeException("geometry value cannot be parsed as Long value");
    }

    @Override
    public double getDoubleValue() {
        throw new RuntimeException("geometry value cannot be parsed as Double value");
    }

    @Override
    public String getRealValue() {
        return getGeometryValue();
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        // code should not be readched, since geometry is analyzed as StringLiteral
        throw new AnalysisException("Unknown check type: " + targetType);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, value);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        value = Text.readString(in);
    }

    public static GeometryLiteral read(DataInput in) throws IOException {
        GeometryLiteral literal = new GeometryLiteral();
        literal.readFields(in);
        return literal;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hashCode(value);
    }
}
