package org.apache.doris.analysis;

/**
 * Created by zhaochun on 2018/4/23.
 */
public class ImportColumnDesc {
    private String column;
    private Expr expr;

    public ImportColumnDesc(String column) {
        this.column = column;
    }

    public ImportColumnDesc(String column, Expr expr) {
        this.column = column;
        this.expr = expr;
    }

    public String getColumn() {
        return column;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean isColumn() {
        return expr == null;
    }
}
