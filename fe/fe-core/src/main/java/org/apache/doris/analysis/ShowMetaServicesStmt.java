package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowStmt;
import org.apache.doris.common.Column;
import org.apache.doris.common.ScalarType;

public class ShowMetaServicesStmt extends ShowStmt {
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("Host", ScalarType.createVarchar(50)));
        builder.addColumn(new Column("Port", ScalarType.createVarchar(10)));
        builder.addColumn(new Column("Role", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Recycler", ScalarType.createVarchar(30)));
        return builder.build();
    }

    @Override
    public String toSql() {
        return "SHOW META-SERVICES";
    }
}
