package org.apache.doris.catalog.external;

import org.apache.doris.catalog.*;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.datasource.ExternalDataSource;
import org.apache.doris.datasource.HMSExternalDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class HMSExternalDatabase extends ExternalDatabase {

    private static final Logger LOG = LogManager.getLogger(HMSExternalDatabase.class);

    private ConcurrentHashMap<String, Long> tableNameToId = new ConcurrentHashMap<>();
    private AtomicLong nextId = new AtomicLong(0);
    private final String hiveMetastoreUri;
    private final TableIf.TableType tableType;

    public HMSExternalDatabase(ExternalDataSource extDataSource, long id, String name, String uri) {
        super(extDataSource, id, name);
        this.hiveMetastoreUri = uri;
        HMSExternalDataSource.HMSType hmsType = ((HMSExternalDataSource) extDataSource).getHmsType();
        if (hmsType.equals(HMSExternalDataSource.HMSType.HIVE)) {
            tableType = TableIf.TableType.HIVE;
        } else if (hmsType.equals(HMSExternalDataSource.HMSType.ICEBERG)) {
            tableType = TableIf.TableType.ICEBERG;
        } else {
            tableType = TableIf.TableType.HUDI;
        }
//        init();
    }

//    private void init() {
//        nameToId = new ConcurrentHashMap<>();
//        List<String> tables = extDataSource.listTableNames(null, name);
//        for (String table : tables) {
//            tableNameToId.put(table, nextId.getAndIncrement());
//        }
//    }

    @Override
    public List<TableIf> getTables() {
        List<TableIf> tables = new ArrayList<>();
        List<String> tableNames = extDataSource.listTableNames(null, name);
        for(String tableName : tableNames) {
            tables.add(new HMSExternalTable(nextId.incrementAndGet(), tableName, name, hiveMetastoreUri, tableType));
        }
        return tables;
    }

    @Override
    public List<TableIf> getTablesOnIdOrder() {
        // Sort the name instead, because the id may change.
        return getTables().stream().sorted(Comparator.comparing(TableIf::getName)).collect(Collectors.toList());
    }

    @Override
    public List<TableIf> getTablesOnIdOrderIfExist(List<Long> tableIdList) {
        return null;
    }

    @Override
    public List<TableIf> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException {
        return null;
    }

    @Override
    public Set<String> getTableNamesWithLock() {
        readLock();
        try {
            return new HashSet<>(extDataSource.listTableNames(null, name));
        } finally {
            readUnlock();
        }
    }

    @Override
    public TableIf getTableNullable(String tableName) {
        if (extDataSource.tableExist(null, name, tableName)){
            return new HMSExternalTable(nextId.incrementAndGet(), tableName, name, hiveMetastoreUri, tableType);
        }
        return null;
    }

    @Override
    public Optional<TableIf> getTable(String tableName) {
        return Optional.ofNullable(getTableNullable(tableName));
    }

    @Override
    public Optional<TableIf> getTable(long tableId) {
        return Optional.empty();
    }

    @Override
    public <E extends Exception> TableIf getTableOrException(String tableName, Function<String, E> e) throws E {
        TableIf table = getTableNullable(tableName);
        if (table == null) {
            throw e.apply(tableName);
        }
        return table;
    }

    @Override
    public <E extends Exception> TableIf getTableOrException(long tableId, Function<Long, E> e) throws E {
        return null;
    }

    @Override
    public TableIf getTableOrMetaException(String tableName) throws MetaNotFoundException {
        return getTableOrException(tableName, t -> new MetaNotFoundException("unknown table, tableName=" + t));
    }

    @Override
    public TableIf getTableOrMetaException(long tableId) throws MetaNotFoundException {
        return null;
    }

    @Override
    public <T extends TableIf> T getTableOrMetaException(String tableName, TableIf.TableType tableType)
        throws MetaNotFoundException {
        TableIf table = getTableOrMetaException(tableName);
        if (table.getType() != tableType) {
            throw new MetaNotFoundException("table type is not " + tableType + ", tableName=" + tableName + ", type=" + table.getType());
        }
        return (T) table;
    }

    @Override
    public <T extends TableIf> T getTableOrMetaException(long tableId, TableIf.TableType tableType)
        throws MetaNotFoundException {
        return null;
    }

    @Override
    public TableIf getTableOrDdlException(String tableName) throws DdlException {
        return getTableOrException(tableName, t -> new DdlException(ErrorCode.ERR_BAD_TABLE_ERROR.formatErrorMsg(t)));
    }

    @Override
    public TableIf getTableOrDdlException(long tableId) throws DdlException {
        return null;
    }

    @Override
    public TableIf getTableOrAnalysisException(String tableName) throws AnalysisException {
        return getTableOrException(tableName, t -> new AnalysisException(ErrorCode.ERR_UNKNOWN_TABLE.formatErrorMsg(t
            , name)));
    }

    @Override
    public OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException {
        return null;
    }

    @Override
    public TableIf getTableOrAnalysisException(long tableId) throws AnalysisException {
        return null;
    }
}
