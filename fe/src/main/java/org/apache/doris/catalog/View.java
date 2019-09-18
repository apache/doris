// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.analysis.ParseNode;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

/**
 * Table metadata representing a catalog view or a local view from a WITH clause.
 * Most methods inherited from Table are not supposed to be called on this class because
 * views are substituted with their underlying definition during analysis of a statement.
 *
 * Refreshing or invalidating a view will reload the view's definition but will not
 * affect the metadata of the underlying tables (if any).
 */
public class View extends Table {
    private static final Logger LOG = LogManager.getLogger(Catalog.class);

    // The original SQL-string given as view definition. Set during analysis.
    // Corresponds to Hive's viewOriginalText.
    private String originalViewDef;

    // Query statement (as SQL string) that defines the View for view substitution.
    // It is a transformation of the original view definition, e.g., to enforce the
    // explicit column definitions even if the original view definition has explicit
    // column aliases.
    // If column definitions were given, then this "expanded" view definition
    // wraps the original view definition in a select stmt as follows.
    //
    // SELECT viewName.origCol1 AS colDesc1, viewName.origCol2 AS colDesc2, ...
    // FROM (originalViewDef) AS viewName
    //
    // Corresponds to Hive's viewExpandedText, but is not identical to the SQL
    // Hive would produce in view creation.
    private String inlineViewDef;

    // View definition created by parsing inlineViewDef_ into a QueryStmt.
    private QueryStmt queryStmt;

    // Set if this View is from a WITH clause and not persisted in the catalog.
    private boolean isLocalView;

    // Set if this View is from a WITH clause with column labels.
    private List<String> colLabels_;

    // Used for read from image
    public View() {
        super(TableType.VIEW);
        isLocalView = false;
    }

    public View(long id, String name, List<Column> schema) {
        super(id, name, TableType.VIEW, schema);
        isLocalView = false;
    }

    /**
     * C'tor for WITH-clause views that already have a parsed QueryStmt and an optional
     * list of column labels.
     */
    public View(String alias, QueryStmt queryStmt, List<String> colLabels) {
        super(-1, alias, TableType.VIEW, null);
        this.isLocalView = true;
        this.queryStmt = queryStmt;
        colLabels_ = colLabels;
    }

    public boolean isLocalView() {
        return isLocalView;
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    public void setOriginalViewDef(String originalViewDef) {
        this.originalViewDef = originalViewDef;
    }

    public void setInlineViewDef(String inlineViewDef) {
        this.inlineViewDef = inlineViewDef;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    /**
     * Initializes the originalViewDef, inlineViewDef, and queryStmt members
     * by parsing the expanded view definition SQL-string.
     * Throws a TableLoadingException if there was any error parsing the
     * the SQL or if the view definition did not parse into a QueryStmt.
     */
    public void init() throws UserException {
        // Parse the expanded view definition SQL-string into a QueryStmt and
        // populate a view definition.
        SqlScanner input = new SqlScanner(new StringReader(inlineViewDef));
        SqlParser parser = new SqlParser(input);
        ParseNode node;
        try {
            node = (ParseNode) parser.parse().value;
        } catch (Exception e) {
            LOG.info("stmt is {}", inlineViewDef);
            LOG.info("exception because: {}", e);
            LOG.info("msg is {}", inlineViewDef);
            // Do not pass e as the exception cause because it might reveal the existence
            // of tables that the user triggering this load may not have privileges on.
            throw new UserException(
                    String.format("Failed to parse view-definition statement of view: %s", name));
        }
        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStmt)) {
            throw new UserException(String.format("View definition of %s " +
                    "is not a query statement", name));
        }
        queryStmt = (QueryStmt) node;
    }

    /**
     * Returns the column labels the user specified in the WITH-clause.
     */
    public List<String> getOriginalColLabels() { return colLabels_; }

    /**
     * Returns the explicit column labels for this view, or null if they need to be derived
     * entirely from the underlying query statement. The returned list has at least as many
     * elements as the number of column labels in the query stmt.
     */
    public List<String> getColLabels() {
        if (colLabels_ == null) return null;
        if (colLabels_.size() >= queryStmt.getColLabels().size()) return colLabels_;
        List<String> explicitColLabels = Lists.newArrayList(colLabels_);
        explicitColLabels.addAll(queryStmt.getColLabels().subList(
                colLabels_.size(), queryStmt.getColLabels().size()));
        return explicitColLabels;
    }

    public boolean hasColLabels() { return colLabels_ != null; }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);

        Text.writeString(out, originalViewDef);
        Text.writeString(out, inlineViewDef);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        originalViewDef = Text.readString(in);
        inlineViewDef = Text.readString(in);
    }
}
