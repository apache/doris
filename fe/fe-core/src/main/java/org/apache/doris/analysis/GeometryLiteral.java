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
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TGeometryLiteral;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class GeometryLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(GeometryLiteral.class);
    private byte[] value;

    public GeometryLiteral() {
        super();
        type = Type.GEOMETRY;
    }

    public GeometryLiteral(byte[] value) throws AnalysisException {
        super();
        this.value = value;
        type = Type.GEOMETRY;
        analysisDone();
    }

    protected GeometryLiteral(GeometryLiteral other) {
        super(other);
        value = other.value;
    }

    @Override
    public Expr clone() {
        return new GeometryLiteral(this);
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        throw new RuntimeException("Not support comparison between GEOMETRY literals");
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.GEOMETRY_LITERAL;
        msg.geometry_literal = new TGeometryLiteral(ByteBuffer.wrap(getValue()));
    }

    @Override
    public String getStringValue() {
        return getValue().toString();
    }

    @Override
    public String getStringValueForArray() {
        return null;
    }

    public byte[] getGeometryValue() {
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
    public byte[] getRealValue() {
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
        int length = value.length;
        out.writeInt(length);
        out.write(value, 0, length);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int length = in.readInt();
        in.readFully(value, 0, length);
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
