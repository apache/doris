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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TDateLiteral;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

public class DateLiteral extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(DateLiteral.class);
    private Date date;

    private DateLiteral() {
        super();
    }

    public DateLiteral(Type type, boolean isMax) throws AnalysisException {
        super();
        this.type = type;
        if (type == Type.DATE) {
            date = isMax ? TimeUtils.MAX_DATE : TimeUtils.MIN_DATE;
        } else {
            date = isMax ? TimeUtils.MAX_DATETIME : TimeUtils.MIN_DATETIME;
        }
        analysisDone();
    }

    public DateLiteral(String s, Type type) throws AnalysisException {
        super();
        init(s, type);
        analysisDone();
    }

    protected DateLiteral(DateLiteral other) {
        super(other);
        date = other.date;
    }

    @Override
    public Expr clone() {
        return new DateLiteral(this);
    }

    public static DateLiteral createMinValue(Type type) {
        DateLiteral dateLiteral = new DateLiteral();
        dateLiteral.type = type;
        if (type == Type.DATE) {
            dateLiteral.date = TimeUtils.MIN_DATE;
        } else {
            dateLiteral.date = TimeUtils.MIN_DATETIME;
        }

        return dateLiteral;
    }

    private void init(String s, Type type) throws AnalysisException {
        Preconditions.checkArgument(type.isDateType());
        date = TimeUtils.parseDate(s, type);
        if (type.isScalarType(PrimitiveType.DATE)) {
            if (date.compareTo(TimeUtils.MAX_DATE) > 0 || date.compareTo(TimeUtils.MIN_DATE) < 0) {
                throw new AnalysisException("Date type column should range from [" + TimeUtils.MIN_DATE + "] to ["
                        + TimeUtils.MAX_DATE + "]");
            }
        } else {
            if (date.compareTo(TimeUtils.MAX_DATETIME) > 0 || date.compareTo(TimeUtils.MIN_DATETIME) < 0) {
                throw new AnalysisException("Datetime type column should range from [" + TimeUtils.MIN_DATETIME
                        + "] to [" + TimeUtils.MAX_DATETIME + "]");
            }
        }
        this.type = type;
    }

    @Override
    public boolean isMinValue() {
        switch (type.getPrimitiveType()) {
            case DATE:
                return this.date.compareTo(TimeUtils.MIN_DATE) == 0;
            case DATETIME:
                return this.date.compareTo(TimeUtils.MIN_DATETIME) == 0;
            default:
                return false;
        }
    }

    @Override
    public Object getRealValue() {
        return TimeUtils.dateTransform(date.getTime(), type);
    }

    // Date column and Datetime column's hash value is not same.
    @Override
    public ByteBuffer getHashValue(PrimitiveType type) {
        String value = TimeUtils.format(date, type);
        ByteBuffer buffer;
        try {
            buffer = ByteBuffer.wrap(value.getBytes("UTF-8"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return buffer;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        if (expr instanceof NullLiteral) {
            return 1;
        }

        if (expr == MaxLiteral.MAX_VALUE) {
            return -1;
        }
        // date time will not overflow when doing addition and subtraction
        return Long.signum(getLongValue() - expr.getLongValue());
    }

    @Override
    public String toSqlImpl() {
        return "'" + getStringValue() + "'";
    }

    @Override
    public String getStringValue() {
        return TimeUtils.format(date, type);
    }

    @Override
    public long getLongValue() {
        return date.getTime();
    }

    @Override
    public double getDoubleValue() {
        return date.getTime();
    }

    @Override
    protected void toThrift(TExprNode msg) {
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(getStringValue());
    }

    public Date getValue() {
        return date;
    }

    @Override
    protected Expr uncheckedCastTo(Type targetType) throws AnalysisException {
        if (targetType.isDateType()) {
            return this;
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue()); 
        }
        Preconditions.checkState(false);
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(date.getTime());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        date = new Date(in.readLong());
    }

    public static DateLiteral read(DataInput in) throws IOException {
        DateLiteral literal = new DateLiteral();
        literal.readFields(in);
        return literal;
    }
}
