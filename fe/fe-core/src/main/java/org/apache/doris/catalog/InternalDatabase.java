package org.apache.doris.catalog;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;

import java.util.*;
import java.util.function.Function;

public interface InternalDatabase<T extends Table> extends DatabaseIf<T> {

    @Override
    public List<T> getTables();

    @Override
    public List<T> getTablesOnIdOrder();

    @Override
    public List<T> getViews();

    @Override
    public List<T> getTablesOnIdOrderIfExist(List<Long> tableIdList);

    @Override
    public List<T> getTablesOnIdOrderOrThrowException(List<Long> tableIdList) throws MetaNotFoundException;

    @Override
    public T getTableNullable(String tableName);

    @Override
    public Optional<T> getTable(String tableName);

    @Override
    public Optional<T> getTable(long tableId);

    @Override
    public <E extends Exception> T getTableOrException(String tableName, Function<String, E> e) throws E;

    @Override
    public <E extends Exception> T getTableOrException(long tableId, Function<Long, E> e) throws E;

    @Override
    public T getTableOrMetaException(String tableName) throws MetaNotFoundException;

    @Override
    public T getTableOrMetaException(long tableId) throws MetaNotFoundException;

    @Override
    public T getTableOrMetaException(String tableName, Table.TableType tableType) throws MetaNotFoundException;

    @Override
    public T getTableOrMetaException(long tableId, Table.TableType tableType) throws MetaNotFoundException;

    @Override
    public T getTableOrDdlException(String tableName) throws DdlException;

    @Override
    public T getTableOrDdlException(long tableId) throws DdlException;

    @Override
    public T getTableOrAnalysisException(String tableName) throws AnalysisException;

    @Override
    public OlapTable getOlapTableOrAnalysisException(String tableName) throws AnalysisException;
    @Override
    public T getTableOrAnalysisException(long tableId) throws AnalysisException;
}
