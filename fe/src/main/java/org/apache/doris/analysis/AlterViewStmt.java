package org.apache.doris.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

// Alter view statement
public class AlterViewStmt extends DdlStmt {

    private List<AlterClause> ops;

    private final boolean ifNotExists;
    private TableName tbl;

    public AlterViewStmt(boolean ifNotExists, TableName tbl, List<AlterClause> ops) {
        this.ifNotExists = ifNotExists;
        this.tbl = tbl;
        this.ops = ops;
    }

    public void setTableName(String newTableName) {
        tbl = new TableName(tbl.getDb(), newTableName);
    }

    public TableName getTbl() {
        return tbl;
    }

    public List<AlterClause> getOps() {
        return ops;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(analyzer);
        if (ops == null || ops.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_ALTER_OPERATION);
        }
        for (AlterClause op : ops) {
            op.analyze(analyzer);
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tbl.getDb(), tbl.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tbl.getTbl());
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER VIEW ").append(tbl.toSql()).append(" ");
        int idx = 0;
        for (AlterClause op : ops) {
            if (idx != 0) {
                sb.append(", \n");
            }
            sb.append(op.toSql());
            idx++;
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
