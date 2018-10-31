package com.baidu.palo.analysis;

import java.util.List;

/**
 * Created by zhaochun on 2018/4/23.
 */
public class ImportColumnsStmt extends StatementBase {
    private List<ImportColumnDesc> columns;

    public ImportColumnsStmt(List<ImportColumnDesc> columns) {
        this.columns = columns;
    }

    public List<ImportColumnDesc> getColumns() {
        return columns;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }
}
