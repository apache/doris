package org.apache.doris.analysis;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BaseViewStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(BaseViewStmt.class);

    protected final TableName tableName;
    protected final List<ColWithComment> cols;
    protected final QueryStmt viewDefStmt;

    // Set during analyze
    protected final List<Column> finalCols;

    protected String originalViewDef;
    protected String inlineViewDef;
    protected QueryStmt cloneStmt;

    public BaseViewStmt(TableName tableName, List<ColWithComment> cols, QueryStmt queryStmt) {
        this.tableName = tableName;
        this.cols = cols;
        this.viewDefStmt = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }


    public List<Column> getColumns() {
        return finalCols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    /**
     * Sets the originalViewDef and the expanded inlineViewDef based on viewDefStmt.
     * If columnNames were given, checks that they do not contain duplicate column names
     * and throws an exception if they do.
     */
    protected void createColumnAndViewDefs(Analyzer analyzer) throws AnalysisException, UserException {
        if (cols != null) {
            if (cols.size() != viewDefStmt.getColLabels().size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            // TODO(zc): type
            for (int i = 0; i < cols.size(); ++i) {
                PrimitiveType type = viewDefStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                Column col = new Column(cols.get(i).getColName(), ScalarType.createType(type));
                col.setComment(cols.get(i).getComment());
                finalCols.add(col);
            }
        } else {
            // TODO(zc): type
            for (int i = 0; i < viewDefStmt.getBaseTblResultExprs().size(); ++i) {
                PrimitiveType type = viewDefStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                finalCols.add(new Column(
                        viewDefStmt.getColLabels().get(i),
                        ScalarType.createType(type)));
            }
        }
        // Set for duplicate columns
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }

        // format view def string
        originalViewDef = viewDefStmt.toSql();

        if (cols == null) {
            inlineViewDef = originalViewDef;
            return;
        }

        Analyzer tmpAnalyzer = new Analyzer(analyzer);
        List<String> colNames = cols.stream().map(c -> c.getColName()).collect(Collectors.toList());
        cloneStmt.substituteSelectList(tmpAnalyzer, colNames);
        inlineViewDef = cloneStmt.toSql();

        // StringBuilder sb = new StringBuilder();
        // sb.append("SELECT ");
        // for (int i = 0; i < columnNames.size(); ++i) {
        //     if (i != 0) {
        //         sb.append(", ");
        //     }
        //     String colRef = viewDefStmt.getColLabels().get(i);
        //     if (!colRef.startsWith("`")) {
        //         colRef = "`" + colRef + "`";
        //     }
        //     String colAlias = finalCols.get(i).getName();

        //     sb.append(String.format("`%s`.%s AS `%s`", tableName.getTbl(), colRef, colAlias));
        // }
        // sb.append(String.format(" FROM (%s) %s", originalViewDef, tableName.getTbl()));
        // inlineViewDef = sb.toString();
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
    }
}
