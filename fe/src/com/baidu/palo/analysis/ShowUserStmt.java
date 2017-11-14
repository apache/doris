package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.common.proc.AccessResourceProcDir;
import com.baidu.palo.qe.ShowResultSetMetaData;

public class ShowUserStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA;

    static {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : AccessResourceProcDir.TITLE_NAMES) {
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
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        user = analyzer.getUser();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

}

