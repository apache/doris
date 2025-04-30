package org.apache.doris.datasource.doris;

import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.InitDatabaseLog;

public class DorisExternalDatabase extends ExternalDatabase<DorisExternalTable> {
    public DorisExternalDatabase(ExternalCatalog extCatalog, long id, String name) {
        super(extCatalog, id, name, InitDatabaseLog.Type.DORIS);
    }

    @Override
    protected DorisExternalTable buildTableForInit(String tableName, long tblId, ExternalCatalog catalog) {
        return new DorisExternalTable(tblId, tableName, name, (DorisExternalCatalog) extCatalog);
    }

    public void addTableForTest(DorisExternalTable tbl) {
        idToTbl.put(tbl.getId(), tbl);
        tableNameToId.put(tbl.getName(), tbl.getId());
    }
}
