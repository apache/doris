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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TStringLiteral;
import org.apache.logging.log4j.LogManager;
import org.apache.doris.catalog.Type;
import org.apache.logging.log4j.Logger;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.VariableVarConverters;

public class JsonLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(JsonLiteral.class);
    private String value;
    // Means the converted session variable need to be cast to int, such as "cast 'STRICT_TRANS_TABLES' to Integer".
    private String beConverted = "";

    public JsonLiteral() {
        super();
        type = Type.JSON;
    }

    public JsonLiteral(String value) {
        super();
        this.value = value;
        type = Type.JSON;
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
        if (expr instanceof NullLiteral) {
            return 1;
        }
        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        // compare string with utf-8 byte array, same with DM,BE,StorageEngine
        byte[] thisBytes = null;
        byte[] otherBytes = null;
        try {
            thisBytes = value.getBytes("UTF-8");
            otherBytes = expr.getStringValue().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            Preconditions.checkState(false);
        }

        int minLength = Math.min(thisBytes.length, otherBytes.length);
        int i = 0;
        for (i = 0; i < minLength; i++) {
            if (thisBytes[i] < otherBytes[i]) {
                return -1;
            } else if (thisBytes[i] > otherBytes[i]) {
                return 1;
            }
        }
        if (thisBytes.length > otherBytes.length) {
            if (thisBytes[i] == 0x00) {
                return 0;
            } else {
                return 1;
            }
        } else if (thisBytes.length < otherBytes.length) {
            if (otherBytes[i] == 0x00) {
                return 0;
            } else {
                return -1;
            }
        } else {
            return 0;
        }
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
        msg.node_type = TExprNodeType.STRING_LITERAL;
        msg.string_literal = new TStringLiteral(getUnescapedValue());
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
        return 0;
    }

    @Override
    public double getDoubleValue() {
        return 0.0;
    }

    @Override
    public String getRealValue() {
        return getJsonValue();
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        // code should not be readched, since JSON is analyzed as StringLiteral
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
