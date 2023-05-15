package org.apache.doris.analysis;

import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TExprNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class IPv4Literal extends LiteralExpr {
    private static final Logger LOG = LogManager.getLogger(IPv4Literal.class);

    public static final int IPV4_MIN = Integer.MIN_VALUE;
    public static final int IPV4_MAX = Integer.MAX_VALUE;

    private int value;

    /**
     * C'tor forcing type, e.g., due to implicit cast
     */
    // for restore
    private IPv4Literal() {
    }

    public IPv4Literal(int value) {
        super();
        init(value);
        analysisDone();
    }

    public IPv4Literal(int value, Type type) throws AnalysisException {
        super();
        checkValueValid(value, type);

        this.value = value;
        this.type = type;
        analysisDone();
    }

    public IPv4Literal(String value, Type type) throws AnalysisException {
        super();
        
    }

    @Override
    protected String toSqlImpl() {
        return null;
    }

    @Override
    protected void toThrift(TExprNode msg) {

    }

    @Override
    public Expr clone() {
        return null;
    }

    @Override
    public boolean isMinValue() {
        return false;
    }

    @Override
    public int compareLiteral(LiteralExpr expr) {
        return 0;
    }

    @Override
    public String getStringValue() {
        return null;
    }

    @Override
    public String getStringValueForArray() {
        return null;
    }
}
