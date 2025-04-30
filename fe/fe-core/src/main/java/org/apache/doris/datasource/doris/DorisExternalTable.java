package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DorisTable;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.SchemaCacheValue;
import org.apache.doris.thrift.TTableDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public class DorisExternalTable extends ExternalTable {
    private static final Logger LOG = LogManager.getLogger(DorisExternalTable.class);

    private DorisTable dorisTable;

    public DorisExternalTable(long id, String name, String dbName, DorisExternalCatalog catalog) {
        super(id, name, catalog, dbName, TableType.DORIS_EXTERNAL_TABLE);
    }

    @Override
    protected synchronized void makeSureInitialized() {
        super.makeSureInitialized();
        if (!objectCreated) {
            dorisTable = toDorisTable();
            objectCreated = true;
        }
    }

    public DorisTable getDorisTable() {
        makeSureInitialized();
        return dorisTable;
    }

    @Override
    public TTableDescriptor toThrift() {
        makeSureInitialized();
        return dorisTable.toThrift();
    }

    @Override
    public Optional<SchemaCacheValue> initSchema() {
        DorisRestClient restClient = ((DorisExternalCatalog) catalog).getDorisRestClient();
        return Optional.of(new SchemaCacheValue(
            DorisUtil.genColumnsFromDoris(restClient, dbName, name)));
    }

    private DorisTable toDorisTable() {
        List<Column> schema = getFullSchema();
        DorisExternalCatalog dorisCatalog = (DorisExternalCatalog) catalog;

        String fullDbName = this.dbName + "." + this.name;
        DorisTable dorisTable = new DorisTable(this.id, fullDbName, schema, TableType.JDBC_EXTERNAL_TABLE);

        dorisTable.setFeNodes(dorisCatalog.getFeNodes());
        dorisTable.setFeArrowNodes(dorisCatalog.getFeArrowNodes());
        dorisTable.setUserName(dorisCatalog.getUsername());
        dorisTable.setPassword(dorisCatalog.getPassword());
        dorisTable.setMaxExecBeNum(dorisCatalog.getMaxExecBeNum());
        dorisTable.setHttpSslEnabled(dorisCatalog.enableSsl());

        dorisTable.setExternalTableName(fullDbName);
        dorisTable.setBeNodes(dorisCatalog.getBeNodes());

        return dorisTable;
    }
}
