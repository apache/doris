package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ColumnType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.AuthProcDir;
import org.apache.doris.qe.ShowResultSetMetaData;

public class ShowUserStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : AuthProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ColumnType.createVarchar(30)));
        }
        META_DATA = builder.build();
    }

    private String user;

    public ShowUserStmt() {

    }

    public String getUser() {
        return user;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        user = analyzer.getQualifiedUser();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}

