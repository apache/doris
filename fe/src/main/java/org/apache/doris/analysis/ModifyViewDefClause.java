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

public class ModifyViewDefClause extends AlterClause {
    private static final Logger LOG = LogManager.getLogger(ModifyViewDefClause.class);

    private final List<ColWithComment> cols;
    private final QueryStmt queryStmt;

    // Set during analyze
    private final List<Column> finalCols;

    private String inlineViewDef;
    private QueryStmt cloneStmt;

    public List<Column> getColumns() {
        return finalCols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public ModifyViewDefClause(List<ColWithComment> cols, QueryStmt queryStmt) {
        this.cols = cols;
        this.queryStmt = queryStmt;
        finalCols = Lists.newArrayList();
    }

    /**
     * Sets the originalViewDef and the expanded inlineViewDef based on viewDefStmt.
     * If columnNames were given, checks that they do not contain duplicate column names
     * and throws an exception if they do.
     */
    private void createColumnAndViewDefs(Analyzer analyzer) throws AnalysisException, UserException {

        if (cols != null) {
            if (cols.size() != queryStmt.getColLabels().size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < cols.size(); ++i) {
                PrimitiveType type = queryStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                Column col = new Column(cols.get(i).getColName(), ScalarType.createType(type));
                col.setComment(cols.get(i).getComment());
                finalCols.add(col);
            }
        } else {
            for (int i = 0; i < queryStmt.getBaseTblResultExprs().size(); ++i) {
                PrimitiveType type = queryStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                finalCols.add(new Column(
                        queryStmt.getColLabels().get(i),
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

        if (cols == null) {
            inlineViewDef = queryStmt.toSql();
        } else {
            Analyzer tmpAnalyzer = new Analyzer(analyzer);
            List<String> colNames = cols.stream().map(c -> c.getColName()).collect(Collectors.toList());
            cloneStmt.substituteSelectList(tmpAnalyzer, colNames);
            inlineViewDef = cloneStmt.toSql();
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (cols != null) {
            cloneStmt = queryStmt.clone();
        }
        queryStmt.setNeedToSql(true);
        Analyzer viewAnalyzer = new Analyzer(analyzer);
        try {
            queryStmt.analyze(viewAnalyzer);
            createColumnAndViewDefs(analyzer);
        } catch (UserException e) {
            throw (AnalysisException) e;
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY DEFINITION");
        if (cols != null) {
            sb.append("(\n");
            for (int i = 0; i < cols.size(); ++i) {
                if (i != 0) {
                    sb.append(", \n");
                }
                sb.append("  ").append(cols.get(i).getColName());
            }
            sb.append("\n)");
        }
        sb.append("AS ");
        sb.append(queryStmt.toSql());
        sb.append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
