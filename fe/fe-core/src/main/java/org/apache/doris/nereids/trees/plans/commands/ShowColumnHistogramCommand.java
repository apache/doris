package org.apache.doris.nereids.trees.plans.commands;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.Histogram;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowColumnHistogramCommand extends ShowCommand {
    private static final ImmutableList<String> TITLE_NAMES =
        new ImmutableList.Builder<String>()
            .add("column_name")
            .add("data_type")
            .add("sample_rate")
            .add("num_buckets")
            .add("buckets")
            .build();

    private TableNameInfo tableNameInfo;

    private final List<String> columnNames;

    private TableIf table;

    public ShowColumnHistogramCommand(TableNameInfo tableNameInfo, List<String> columnNames) {
        super(PlanType.SHOW_COLUMN_HISTOGRAM);
        this.tableNameInfo = tableNameInfo;
        this.columnNames = columnNames;
    }

    public TableNameInfo getTableName() {
        return tableNameInfo;
    }

    public void validate(ConnectContext ctx) throws UserException {
        tableNameInfo.analyze(ctx);

        // disallow external catalog
        Util.prohibitExternalCatalog(tableNameInfo.getCtl(), this.getClass().getSimpleName());
        CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableNameInfo.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException("Catalog: {} not exists", tableNameInfo.getCtl());
        }
        DatabaseIf<TableIf> db = catalog.getDb(tableNameInfo.getDb()).orElse(null);
        if (db == null) {
            ErrorReport.reportAnalysisException("DB: {} not exists", tableNameInfo.getDb());
        }
        table = db.getTable(tableNameInfo.getTbl()).orElse(null);
        if (table == null) {
            ErrorReport.reportAnalysisException("Table: {} not exists", tableNameInfo.getTbl());
        }

        if (!Env.getCurrentEnv().getAccessManager()
            .checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(), tableNameInfo.getDb(), tableNameInfo.getTbl(),
                    PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
        }

        if (columnNames != null) {
            Optional<Column> nullColumn = columnNames.stream()
                .map(table::getColumn)
                .filter(Objects::isNull)
                .findFirst();
            if (nullColumn.isPresent()) {
                ErrorReport.reportAnalysisException("Column: {} not exists", nullColumn.get());
            }
        }
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    public TableIf getTable() {
        return table;
    }

    public ShowResultSet constructResultSet(List<Pair<String, Histogram>> columnStatistics) {
        List<List<String>> result = Lists.newArrayList();
        columnStatistics.forEach(p -> {
            if (p.second == null || p.second.dataType == Type.NULL) {
                return;
            }
            List<String> row = Lists.newArrayList();
            row.add(p.first);
            row.add(String.valueOf(p.second.dataType));
            row.add(String.valueOf(p.second.sampleRate));
            row.add(String.valueOf(p.second.numBuckets));
            row.add(Histogram.getBucketsJson(p.second.buckets).toString());
            result.add(row);
        });

        return new ShowResultSet(getMetaData(), result);
    }

    public Set<String> getColumnNames() {
        if (columnNames != null) {
            return  Sets.newHashSet(columnNames);
        }
        return table.getColumns().stream()
                .map(Column::getName).collect(Collectors.toSet());
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return null;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return null;
    }
}
