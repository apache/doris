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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.IOException;
import java.io.StringReader;
import java.lang.ref.SoftReference;
import java.util.List;

/**
 * Table metadata representing a catalog view or a local view from a WITH clause.
 * Most methods inherited from Table are not supposed to be called on this class because
 * views are substituted with their underlying definition during analysis of a statement.
 *
 * Refreshing or invalidating a view will reload the view's definition but will not
 * affect the metadata of the underlying tables (if any).
 */
public class View extends Table implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(View.class);

    // The original SQL-string given as view definition. Set during analysis.
    // Corresponds to Hive's viewOriginalText.
    @Deprecated
    private String originalViewDef = "";

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
    @SerializedName("ivd")
    private String inlineViewDef;

    // for persist
    @SerializedName("sm")
    private long sqlMode = 0L;

    // View definition created by parsing inlineViewDef_ into a QueryStmt.
    // 'queryStmt' is a strong reference, which is used when this view is created directly from a QueryStmt
    // 'queryStmtRef' is a soft reference, it is created from parsing query stmt, and it will be cleared if
    // JVM memory is not enough.
    private QueryStmt queryStmt;
    private SoftReference<QueryStmt> queryStmtRef = new SoftReference<QueryStmt>(null);

    // Set if this View is from a WITH clause and not persisted in the catalog.
    private boolean isLocalView;

    // Set if this View is from a WITH clause with column labels.
    private List<String> colLabels;

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
        this.colLabels = colLabels;
    }

    public boolean isLocalView() {
        return isLocalView;
    }

    public QueryStmt getQueryStmt() {
        if (queryStmt != null) {
            return queryStmt;
        }
        QueryStmt retStmt = queryStmtRef.get();
        if (retStmt == null) {
            synchronized (this) {
                retStmt = queryStmtRef.get();
                if (retStmt == null) {
                    try {
                        retStmt = init();
                    } catch (UserException e) {
                        // should not happen
                        LOG.error("unexpected exception", e);
                    }
                }
            }
        }
        return retStmt;
    }

    public void setInlineViewDefWithSqlMode(String inlineViewDef, long sqlMode) {
        this.inlineViewDef = inlineViewDef;
        this.sqlMode = sqlMode;
    }

    public void setSqlMode(long sqlMode) {
        this.sqlMode = sqlMode;
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
    public synchronized QueryStmt init() throws UserException {
        Preconditions.checkNotNull(inlineViewDef);
        // Parse the expanded view definition SQL-string into a QueryStmt and
        // populate a view definition.
        SqlScanner input = new SqlScanner(new StringReader(inlineViewDef), sqlMode);
        SqlParser parser = new SqlParser(input);
        ParseNode node;
        try {
            node = (ParseNode) SqlParserUtils.getFirstStmt(parser);
        } catch (Exception e) {
            // Do not pass e as the exception cause because it might reveal the existence
            // of tables that the user triggering this load may not have privileges on.
            throw new UserException(
                    String.format("Failed to parse view-definition statement of view: %s, stmt is %s, reason is %s",
                            name, inlineViewDef, e.getMessage()));
        }
        // Make sure the view definition parses to a query statement.
        if (!(node instanceof QueryStmt)) {
            throw new UserException(String.format("View definition of %s "
                    + "is not a query statement", name));
        }
        queryStmtRef = new SoftReference<QueryStmt>((QueryStmt) node);
        return (QueryStmt) node;
    }

    /**
     * Returns the column labels the user specified in the WITH-clause.
     */
    public List<String> getOriginalColLabels() {
        return colLabels;
    }

    /**
     * Returns the explicit column labels for this view, or null if they need to be derived
     * entirely from the underlying query statement. The returned list has at least as many
     * elements as the number of column labels in the query stmt.
     */
    public List<String> getColLabels() {
        QueryStmt stmt = getQueryStmt();
        if (colLabels == null) {
            return null;
        }
        if (colLabels.size() >= stmt.getColLabels().size()) {
            return colLabels;
        }
        List<String> explicitColLabels = Lists.newArrayList(colLabels);
        explicitColLabels.addAll(stmt.getColLabels().subList(colLabels.size(), stmt.getColLabels().size()));
        return explicitColLabels;
    }

    public boolean hasColLabels() {
        return colLabels != null;
    }

    // Get the md5 of signature string of this view.
    // This method is used to determine whether the views have the same schema.
    // Contains:
    // view name, type, full schema, inline view def, sql mode
    @Override
    public String getSignature(int signatureVersion) {
        StringBuilder sb = new StringBuilder(signatureVersion);
        sb.append(name);
        sb.append(type);
        sb.append(Util.getSchemaSignatureString(fullSchema));
        sb.append(inlineViewDef);

        // ATTN: sqlMode is missing when persist view, so we should not append it here.
        //
        // To keep compatible with the old version, without sqlMode, if the signature of views
        // are the same, we think the should has the same sqlMode. (since the sqlMode doesn't
        // effect the parsing of inlineViewDef, otherwise the parsing will fail),
        //
        // sb.append(sqlMode);
        String md5 = DigestUtils.md5Hex(sb.toString());
        if (LOG.isDebugEnabled()) {
            LOG.debug("get signature of view {}: {}. signature string: {}", name, md5, sb.toString());
        }
        return md5;
    }

    @Override
    public View clone() {
        View copied = DeepCopy.copy(this, View.class, FeConstants.meta_version);
        if (copied == null) {
            LOG.warn("failed to copy view: " + getName());
            return null;
        }
        copied.setSqlMode(this.sqlMode);
        return copied;
    }

    public static View read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_136) {
            View t = new View();
            t.readFields(in);
            return t;
        }
        return GsonUtils.GSON.fromJson(Text.readString(in), View.class);
    }

    public void resetIdsForRestore(Env env, String srcDbName, String dbName) {
        id = env.getNextId();

        // the source db name is not setted in old BackupMeta, keep compatible with the old one.
        if (srcDbName != null) {
            inlineViewDef = inlineViewDef.replaceAll(srcDbName, dbName);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        originalViewDef = "";
    }

    @Deprecated
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        // just do not want to modify the meta version, so leave originalViewDef here but set it as empty
        originalViewDef = Text.readString(in);
        originalViewDef = "";
        inlineViewDef = Text.readString(in);
    }
}
