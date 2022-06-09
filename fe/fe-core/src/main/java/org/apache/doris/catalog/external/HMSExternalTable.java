package org.apache.doris.catalog.external;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.HiveMetaStoreClientHelper;
import org.apache.doris.common.DdlException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class HMSExternalTable extends ExternalTable {

    private static final Logger LOG = LogManager.getLogger(HMSExternalTable.class);

    private final String metastoreUri;
    private final String dbName;
    private org.apache.hadoop.hive.metastore.api.Table remoteTable = null;

    public HMSExternalTable(long id, String name, String dbName, String uri, TableType type) {
        super(id, name, type);
        this.dbName = dbName;
        this.metastoreUri = uri;
    }

    public org.apache.hadoop.hive.metastore.api.Table getRemoteTable() {
        if (remoteTable == null) {
            synchronized (this) {
                if (remoteTable == null) {
                    try {
                        remoteTable = HiveMetaStoreClientHelper.getTable(dbName, name, metastoreUri);
                    } catch (DdlException e) {
                        LOG.warn("Fail to get remote hive table. db {}, table {}, uri {}", dbName, name, metastoreUri);
                        remoteTable = null;
                    }
                }
            }
        }
        return remoteTable;
    }

    @Override
    public List<Column> getFullSchema() {
        List<Column> schema = new ArrayList<>();
        try {
            for (FieldSchema field : HiveMetaStoreClientHelper.getSchema(dbName, name, metastoreUri)) {
                schema.add(new Column(field.getName(), HiveMetaStoreClientHelper.hiveTypeToDorisType(field.getType()),
                    true, null, true, null, field.getComment()));
            }
        } catch (DdlException e) {
            LOG.warn("Fail to get schema of hms table {}", name, e);
        }
        synchronized (this) {
            fullSchema = schema;
        }
        return schema;
    }

    @Override
    public List<Column> getBaseSchema() {
        return getFullSchema();
    }

    @Override
    public List<Column> getBaseSchema(boolean full) {
        return getFullSchema();
    }

    @Override
    public Column getColumn(String name) {
        if (fullSchema == null) {
            getFullSchema();
        }
        synchronized (this) {
            for (Column column : fullSchema) {
                if (name.equals(column.getName())) {
                    return column;
                }
            }
        }
        return null;
    }
}
