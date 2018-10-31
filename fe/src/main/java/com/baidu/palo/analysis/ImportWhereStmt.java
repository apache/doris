package com.baidu.palo.analysis;

/**
 * Created by zhaochun on 2018/4/24.
 */
public class ImportWhereStmt extends StatementBase {
    private Expr expr;

    public ImportWhereStmt(Expr expr) {
        this.expr = expr;
    }

    public Expr getExpr() {
        return expr;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}
