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

package org.apache.doris.analysis;

import org.apache.commons.collections.CollectionUtils;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.rewrite.ExprRewriter;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Superclass of all table references, including references to views, base tables
 * (Hdfs, HBase or DataSource tables), and nested collections. Contains the join
 * specification. An instance of a TableRef (and not a subclass thereof) represents
 * an unresolved table reference that must be resolved during analysis. All resolved
 * table references are subclasses of TableRef.
 *
 * The analysis of table refs follows a two-step process:
 *
 * 1. Resolution: A table ref's path is resolved and then the generic TableRef is
 * replaced by a concrete table ref (a BaseTableRef, CollectionTableRef or ViewRef)
 * in the originating stmt and that is given the resolved path. This step is driven by
 * Analyzer.resolveTableRef().
 *
 * 2. Analysis/registration: After resolution, the concrete table ref is analyzed
 * to register a tuple descriptor for its resolved path and register other table-ref
 * specific state with the analyzer (e.g., whether it is outer/semi joined, etc.).
 *
 * Therefore, subclasses of TableRef should never call the analyze() of its superclass.
 *
 * TODO for 2.3: The current TableRef class hierarchy and the related two-phase analysis
 * feels convoluted and is hard to follow. We should reorganize the TableRef class
 * structure for clarity of analysis and avoid a table ref 'switching genders' in between
 * resolution and registration.
 *
 * TODO for 2.3: Rename this class to CollectionRef and re-consider the naming and
 * structure of all subclasses.
 */
public class TableRef implements ParseNode, Writable {
    private static final Logger LOG = LogManager.getLogger(TableRef.class);
    protected TableName name;
    private PartitionNames partitionNames = null;

    // Legal aliases of this table ref. Contains the explicit alias as its sole element if
    // there is one. Otherwise, contains the two implicit aliases. Implicit aliases are set
    // in the c'tor of the corresponding resolved table ref (subclasses of TableRef) during
    // analysis. By convention, for table refs with multiple implicit aliases, aliases_[0]
    // contains the fully-qualified implicit alias to ensure that aliases_[0] always
    // uniquely identifies this table ref regardless of whether it has an explicit alias.
    protected String[] aliases_;

    // Indicates whether this table ref is given an explicit alias,
    protected boolean hasExplicitAlias_;

    protected JoinOperator joinOp;
    protected List<String> usingColNames;
    private ArrayList<String> joinHints;
    private ArrayList<String> sortHints;
    private ArrayList<String> commonHints; //The Hints is set by user
    private boolean isForcePreAggOpened;
    // ///////////////////////////////////////
    // BEGIN: Members that need to be reset()
    
    protected Expr onClause;
    
    // the ref to the left of us, if we're part of a JOIN clause
    protected TableRef leftTblRef;

    // true if this TableRef has been analyzed; implementing subclass should set it to true
    // at the end of analyze() call.
    protected boolean isAnalyzed;

    // Lists of table ref ids and materialized tuple ids of the full sequence of table
    // refs up to and including this one. These ids are cached during analysis because
    // we may alter the chain of table refs during plan generation, but we still rely
    // on the original list of ids for correct predicate assignment.
    // Populated in analyzeJoin().
    protected List<TupleId> allTableRefIds_ = Lists.newArrayList();
    protected List<TupleId> allMaterializedTupleIds_ = Lists.newArrayList();

    // All physical tuple ids that this table ref is correlated with:
    // Tuple ids of root descriptors from outer query blocks that this table ref
    // (if a CollectionTableRef) or contained CollectionTableRefs (if an InlineViewRef)
    // are rooted at. Populated during analysis.
    protected List<TupleId> correlatedTupleIds_ = Lists.newArrayList();

    // analysis output
    protected TupleDescriptor desc;

    // set after analyzeJoinHints(); true if explicitly set via hints
    private boolean isBroadcastJoin;
    private boolean isPartitionJoin;
    private String sortColumn = null;

    // END: Members that need to be reset()
    // ///////////////////////////////////////
    
    public TableRef() {
        // for persist
    }

    public TableRef(TableName name, String alias) {
        this(name, alias, null);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames) {
        this(name, alias, partitionNames, null);
    }

    public TableRef(TableName name, String alias, PartitionNames partitionNames, ArrayList<String> commonHints) {
        this.name = name;
        if (alias != null) {
            aliases_ = new String[] { alias };
            hasExplicitAlias_ = true;
        } else {
            hasExplicitAlias_ = false;
        }
        this.partitionNames = partitionNames;
        this.commonHints = commonHints;
        isAnalyzed = false;
    }
    // Only used to clone
    // this will reset all the 'analyzed' stuff
    protected TableRef(TableRef other) {
        name = other.name;
        aliases_ = other.aliases_;
        hasExplicitAlias_ = other.hasExplicitAlias_;
        joinOp = other.joinOp;
        // NOTE: joinHints and sortHints maybe changed after clone. so we new one List.
        joinHints =
                (other.joinHints != null) ? Lists.newArrayList(other.joinHints) : null;
        sortHints =
                (other.sortHints != null) ? Lists.newArrayList(other.sortHints) : null;
        onClause = (other.onClause != null) ? other.onClause.clone().reset() : null;
        partitionNames = (other.partitionNames != null) ? new PartitionNames(other.partitionNames) : null;
        commonHints = other.commonHints;

        usingColNames =
                (other.usingColNames != null) ? Lists.newArrayList(other.usingColNames) : null;
        // The table ref links are created at the statement level, so cloning a set of linked
        // table refs is the responsibility of the statement.
        leftTblRef = null;
        isAnalyzed = other.isAnalyzed;
        allTableRefIds_ = Lists.newArrayList(other.allTableRefIds_);
        allMaterializedTupleIds_ = Lists.newArrayList(other.allMaterializedTupleIds_);
        correlatedTupleIds_ = Lists.newArrayList(other.correlatedTupleIds_);
        desc = other.desc;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNRESOLVED_TABLE_REF, tableRefToSql());
    }

    /**
     * Creates and returns a empty TupleDescriptor registered with the analyzer. The
     * returned tuple descriptor must have its source table set via descTbl.setTable()).
     * This method is called from the analyzer when registering this table reference.
     */
    public TupleDescriptor createTupleDescriptor(Analyzer analyzer) throws AnalysisException {
        ErrorReport.reportAnalysisException(ErrorCode.ERR_UNRESOLVED_TABLE_REF, tableRefToSql());
        return null;
    }

    public JoinOperator getJoinOp() {
        // if it's not explicitly set, we're doing an inner join
        return (joinOp == null ? JoinOperator.INNER_JOIN : joinOp);
    }

    public void setJoinOp(JoinOperator op) {
        this.joinOp = op;
    }

    public Expr getOnClause() {
        return onClause;
    }

    public void setOnClause(Expr e) {
        this.onClause = e;
    }

    public TableName getName() {
        return name;
    }

    /**
     * This method should only be called after the TableRef has been analyzed.
     */
    public TupleDescriptor getDesc() {
        Preconditions.checkState(isAnalyzed);
        // after analyze(), desc should be set.
        Preconditions.checkState(desc != null);
        return desc;
    }

    /**
     * This method should only be called after the TableRef has been analyzed.
     */
    public TupleId getId() {
        Preconditions.checkState(isAnalyzed);
        // after analyze(), desc should be set.
        Preconditions.checkState(desc != null);
        return desc.getId();
    }

    /**
     * Return the list of of materialized tuple ids from the TableRef.
     * This method should only be called after the TableRef has been analyzed.
     */
    public List<TupleId> getMaterializedTupleIds() {
        // This function should only be called after analyze().
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc.getId().asList();
    }

    /**
     * Return the list of tuple ids materialized by the full sequence of
     * table refs up to this one.
     */
    public List<TupleId> getAllMaterializedTupleIds() {
        if (leftTblRef != null) {
            List<TupleId> result = Lists.newArrayList(leftTblRef.getAllMaterializedTupleIds());
            result.addAll(getMaterializedTupleIds());
            return result;
        } else {
            return getMaterializedTupleIds();
        }
    }

    /**
     * Returns true if this table ref has a resolved path that is rooted at a registered
     * tuple descriptor, false otherwise.
     */
    public boolean isRelative() { return false; }

    /**
     * Indicates if this TableRef directly or indirectly references another TableRef from
     * an outer query block.
     */
    public boolean isCorrelated() { return !correlatedTupleIds_.isEmpty(); }

    public Table getTable() {
        return desc.getTable();
    }

    public void setUsingClause(List<String> colNames) {
        this.usingColNames = colNames;
    }

    public TableRef getLeftTblRef() {
        return leftTblRef;
    }

    public void setLeftTblRef(TableRef leftTblRef) {
        this.leftTblRef = leftTblRef;
    }

    public ArrayList<String> getJoinHints() {
        return joinHints;
    }

    public boolean hasJoinHints() {
        return CollectionUtils.isNotEmpty(joinHints);
    }

    public void setJoinHints(ArrayList<String> hints) {
        this.joinHints = hints;
    }

    public boolean isBroadcastJoin() {
        return isBroadcastJoin;
    }

    public boolean isPartitionJoin() {
        return isPartitionJoin;
    }

    public boolean isForcePreAggOpened() {
        return isForcePreAggOpened;
    }

    public void setSortHints(ArrayList<String> hints) {
        this.sortHints = hints;
    }

    public String getSortColumn() {
        return sortColumn;
    }

    protected void analyzeSortHints() throws AnalysisException {
        if (sortHints == null) {
            return;
        }
        for (String hint : sortHints) {
            sortColumn = hint;
        }
    }

    /**
     * Parse hints.
     */
    private void analyzeJoinHints() throws AnalysisException {
        if (joinHints == null) {
            return;
        }
        for (String hint : joinHints) {
            if (hint.toUpperCase().equals("BROADCAST")) {
                if (joinOp == JoinOperator.RIGHT_OUTER_JOIN
                        || joinOp == JoinOperator.FULL_OUTER_JOIN
                        || joinOp == JoinOperator.RIGHT_SEMI_JOIN
                        || joinOp == JoinOperator.RIGHT_ANTI_JOIN) {
                    throw new AnalysisException(
                            joinOp.toString() + " does not support BROADCAST.");
                }
                if (isPartitionJoin) {
                    throw new AnalysisException("Conflicting JOIN hint: " + hint);
                }
                isBroadcastJoin = true;
            } else if (hint.toUpperCase().equals("SHUFFLE")) {
                if (joinOp == JoinOperator.CROSS_JOIN) {
                    throw new AnalysisException("CROSS JOIN does not support SHUFFLE.");
                }
                if (isBroadcastJoin) {
                    throw new AnalysisException("Conflicting JOIN hint: " + hint);
                }
                isPartitionJoin = true;
            } else {
                throw new AnalysisException("JOIN hint not recognized: " + hint);
            }
        }
    }

    /**
     * Parse PreAgg hints.
     */
    protected void analyzeHints() throws AnalysisException {
        if (commonHints == null || commonHints.isEmpty()) {
            return;
        }
        // Currently only 'PREAGGOPEN' is supported
        for (String hint : commonHints) {
            if (hint.toUpperCase().equals("PREAGGOPEN")) {
                isForcePreAggOpened = true;
                break;
            }
        }
    }

    /**
     * Analyze the join clause.
     * The join clause can only be analyzed after the left table has been analyzed
     * and the TupleDescriptor (desc) of this table has been created.
     */
    public void analyzeJoin(Analyzer analyzer)  throws AnalysisException {
        Preconditions.checkState(leftTblRef == null || leftTblRef.isAnalyzed);
        Preconditions.checkState(desc != null);
        analyzeJoinHints();

        // Populate the lists of all table ref and materialized tuple ids.
        allTableRefIds_.clear();
        allMaterializedTupleIds_.clear();
        if (leftTblRef != null) {
            allTableRefIds_.addAll(leftTblRef.getAllTableRefIds());
            allMaterializedTupleIds_.addAll(leftTblRef.getAllMaterializedTupleIds());
        }
        allTableRefIds_.add(getId());
        allMaterializedTupleIds_.addAll(getMaterializedTupleIds());

        if (usingColNames != null) {
            // Turn USING clause into equivalent ON clause.
            Preconditions.checkState(onClause == null);
            for (String colName : usingColNames) {
                // check whether colName exists both for our table and the one
                // to the left of us
                if (leftTblRef.getDesc().getTable().getColumn(colName) == null) {
                    throw new AnalysisException("Unknown column " + colName + " for alias " + leftTblRef.getAlias()
                            + " (in" + " \"" + this.toSql() + "\")");
                }
                if (desc.getTable().getColumn(colName) == null) {
                    throw new AnalysisException("Unknown column " + colName + " for alias " + getAlias() + " (in \"" +
                            this.toSql() + "\")");
                }

                // create predicate "<left>.colName = <right>.colName"
                BinaryPredicate eqPred = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                        new SlotRef(leftTblRef.getAliasAsName(), colName),
                        new SlotRef(getAliasAsName(), colName));
                if (onClause == null) {
                    onClause = eqPred;
                } else {
                    onClause = new CompoundPredicate(CompoundPredicate.Operator.AND, onClause, eqPred);
                }
            }
        }

        // at this point, both 'this' and leftTblRef have been analyzed
        // and registered
        boolean lhsIsNullable = false;
        boolean rhsIsNullable = false;

        // at this point, both 'this' and leftTblRef have been analyzed and registered;
        // register the tuple ids of the TableRefs on the nullable side of an outer join
        if (joinOp == JoinOperator.LEFT_OUTER_JOIN
                || joinOp == JoinOperator.FULL_OUTER_JOIN) {
            analyzer.registerOuterJoinedTids(getId().asList(), this);
        }
        if (joinOp == JoinOperator.RIGHT_OUTER_JOIN
                || joinOp == JoinOperator.FULL_OUTER_JOIN) {
            analyzer.registerOuterJoinedTids(leftTblRef.getAllTableRefIds(), this);
        }
        // register the tuple ids of a full outer join
        if (joinOp == JoinOperator.FULL_OUTER_JOIN) {
            analyzer.registerFullOuterJoinedTids(leftTblRef.getAllTableRefIds(), this);
            analyzer.registerFullOuterJoinedTids(getId().asList(), this);
        }
        // register the tuple id of the rhs of a left semi join
        TupleId semiJoinedTupleId = null;
        if (joinOp == JoinOperator.LEFT_SEMI_JOIN
                || joinOp == JoinOperator.LEFT_ANTI_JOIN
                || joinOp == JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN) {
            analyzer.registerSemiJoinedTid(getId(), this);
            semiJoinedTupleId = getId();
        }
        // register the tuple id of the lhs of a right semi join
        if (joinOp == JoinOperator.RIGHT_SEMI_JOIN
                || joinOp == JoinOperator.RIGHT_ANTI_JOIN) {
            analyzer.registerSemiJoinedTid(leftTblRef.getId(), this);
            semiJoinedTupleId = leftTblRef.getId();
        }

        //  right anti join use shuffle , basecase broadcast join can't support right anti
        if (joinOp == JoinOperator.RIGHT_ANTI_JOIN || joinOp == JoinOperator.RIGHT_SEMI_JOIN) {
            isPartitionJoin = true;
            isBroadcastJoin = false;
        }

        // cross join can't be used with ON clause
        if (onClause != null && joinOp == JoinOperator.CROSS_JOIN) {
            throw new AnalysisException("Cross join can't be used with ON clause");
        }

        if (onClause != null) {
            analyzer.setVisibleSemiJoinedTuple(semiJoinedTupleId);
            onClause.analyze(analyzer);
            analyzer.setVisibleSemiJoinedTuple(null);
            onClause.checkReturnsBool("ON clause", true);
            if (onClause.contains(Expr.isAggregatePredicate())) {
                throw new AnalysisException(
                        "aggregate function not allowed in ON clause: " + toSql());
            }
            if (onClause.contains(AnalyticExpr.class)) {
                throw new AnalysisException(
                        "analytic expression not allowed in ON clause: " + toSql());
            }
            Set<TupleId> onClauseTupleIds = Sets.newHashSet();
            List<Expr> conjuncts = onClause.getConjuncts();
            // Outer join clause conjuncts are registered for this particular table ref
            // (ie, can only be evaluated by the plan node that implements this join).
            // The exception are conjuncts that only pertain to the nullable side
            // of the outer join; those can be evaluated directly when materializing tuples
            // without violating outer join semantics.
            analyzer.registerOnClauseConjuncts(conjuncts, this);
            for (Expr e: conjuncts) {
                List<TupleId> tupleIds = Lists.newArrayList();
                e.getIds(tupleIds, null);
                onClauseTupleIds.addAll(tupleIds);
            }
        } else if (!isRelative() && !isCorrelated()
                && (getJoinOp().isOuterJoin() || getJoinOp().isSemiJoin())) {
            throw new AnalysisException(
                    joinOp.toString() + " requires an ON or USING clause.");
        } else {
            // Indicate that this table ref has an empty ON-clause.
            analyzer.registerOnClauseConjuncts(Collections.<Expr>emptyList(), this);
        }
    }

    public void rewriteExprs(ExprRewriter rewriter, Analyzer analyzer)
            throws AnalysisException {
        Preconditions.checkState(isAnalyzed);
        if (onClause != null) onClause = rewriter.rewrite(onClause, analyzer);
    }

    private String joinOpToSql() {
        Preconditions.checkState(joinOp != null);
        switch (joinOp) {
            case INNER_JOIN:
                return "INNER JOIN";
            case LEFT_OUTER_JOIN:
                return "LEFT OUTER JOIN";
            case LEFT_SEMI_JOIN:
                return "LEFT SEMI JOIN";
            case LEFT_ANTI_JOIN:
                return "LEFT ANTI JOIN";
            case RIGHT_SEMI_JOIN:
                return "RIGHT SEMI JOIN";
            case RIGHT_ANTI_JOIN:
                return "RIGHT ANTI JOIN";
            case RIGHT_OUTER_JOIN:
                return "RIGHT OUTER JOIN";
            case FULL_OUTER_JOIN:
                return "FULL OUTER JOIN";
            case CROSS_JOIN:
                return "CROSS JOIN";
            case MERGE_JOIN:
                return "MERGE JOIN";
            default:
                return "bad join op: " + joinOp.toString();
        }
    }

    /**
     * Return the list of table ref ids of the full sequence of table refs up to
     * and including this one.
     */
    public List<TupleId> getAllTableRefIds() {
        Preconditions.checkState(isAnalyzed);
        return allTableRefIds_;
    }

    /**
     * Return the table ref presentation to be used in the toSql string
     */
    public String tableRefToSql() {
        String aliasSql = null;
        String alias = getExplicitAlias();
        if (alias != null) aliasSql = ToSqlUtils.getIdentSql(alias);

        // TODO(zc):
        // List<String> path = rawPath_;
        // if (resolvedPath_ != null) path = resolvedPath_.getFullyQualifiedRawPath();
        // return ToSqlUtils.getPathSql(path) + ((aliasSql != null) ? " " + aliasSql : "");

        return name.toSql() + ((aliasSql != null) ? " " + aliasSql : "");
    }

    @Override
    public String toSql() {
        if (joinOp == null) {
            // prepend "," if we're part of a sequence of table refs w/o an
            // explicit JOIN clause
            return (leftTblRef != null ? ", " : "") + tableRefToSql();
        }

        StringBuilder output = new StringBuilder(" " + joinOpToSql() + " ");
        if (joinHints != null && !joinHints.isEmpty()) {
            output.append("[").append(Joiner.on(", ").join(joinHints)).append("] ");
        }
        output.append(tableRefToSql()).append(" ");
        if (usingColNames != null) {
            output.append("USING (").append(Joiner.on(", ").join(usingColNames)).append(")");
        } else if (onClause != null) {
            output.append("ON ").append(onClause.toSql());
        }
        return output.toString();
    }

    public String getAlias() {
        if (!hasExplicitAlias()) {
            return name.toString();
        }
        return getUniqueAlias();
    }

    public TableName getAliasAsName() {
        if (hasExplicitAlias()) {
            return new TableName(null, getUniqueAlias());
        }
        return name;
    }

    /**
     * Returns all legal aliases of this table ref.
     */
    public String[] getAliases() { return aliases_; }

    /**
     * Returns the explicit alias or the fully-qualified implicit alias. The returned alias
     * is guaranteed to be unique (i.e., column/field references against the alias cannot
     * be ambiguous).
     */
    public String getUniqueAlias() { return aliases_[0]; }

    /**
     * Returns true if this table ref has an explicit alias.
     * Note that getAliases().length() == 1 does not imply an explicit alias because
     * nested collection refs have only a single implicit alias.
     */
    public boolean hasExplicitAlias() { return hasExplicitAlias_; }

    /**
     * Returns the explicit alias if this table ref has one, null otherwise.
     */
    public String getExplicitAlias() {
        if (hasExplicitAlias()) return getUniqueAlias();
        return null;
    }

    public boolean isAnalyzed() { return isAnalyzed; }
    public boolean isResolved() {
        return !getClass().equals(TableRef.class);
    }

    /**
     * Return the list of tuple ids of the full sequence of table refs up to this one.
     */
    public List<TupleId> getAllTupleIds() {
        Preconditions.checkState(isAnalyzed);
        if (leftTblRef != null) {
            List<TupleId> result = leftTblRef.getAllTupleIds();
            result.add(desc.getId());
            return result;
        } else {
            return Lists.newArrayList(desc.getId());
        }
    }

    /**
     * Set this table's context-dependent join attributes from the given table.
     * Does not clone the attributes.
     */
    protected void setJoinAttrs(TableRef other) {
        this.joinOp = other.joinOp;
        this.joinHints = other.joinHints;
        // this.tableHints_ = other.tableHints_;
        this.onClause = other.onClause;
        this.usingColNames = other.usingColNames;
    }

    public void reset() {
        isAnalyzed = false;
        //  resolvedPath_ = null;
        if (usingColNames != null) {
            // The using col names are converted into an on-clause predicate during analysis,
            // so unset the on-clause here.
            onClause = null;
        } else if (onClause != null) {
            onClause.reset();
        }
        leftTblRef = null;
        allTableRefIds_.clear();
        allMaterializedTupleIds_.clear();
        correlatedTupleIds_.clear();
        desc = null;
    }

    /**
     * Returns a deep clone of this table ref without also cloning the chain of table refs.
     * Sets leftTblRef_ in the returned clone to null.
     */
    @Override
    public TableRef clone() {
        return new TableRef(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        if (partitionNames != null) {
            sb.append(partitionNames.toSql());
        }
        if (aliases_ != null && aliases_.length > 0) {
            sb.append(" AS ").append(aliases_[0]);
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        name.write(out);
        if (partitionNames == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            partitionNames.write(out);
        }
        
        if (hasExplicitAlias()) {
            out.writeBoolean(true);
            Text.writeString(out, getExplicitAlias());
        } else {
            out.writeBoolean(false);
        }
    }

    public void readFields(DataInput in) throws IOException {
        name = new TableName();
        name.readFields(in);
        if (in.readBoolean()) {
            if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_77) {
                List<String> partitions = Lists.newArrayList();
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    String partName = Text.readString(in);
                    partitions.add(partName);
                }
                partitionNames = new PartitionNames(false, partitions);
            } else {
                partitionNames = PartitionNames.read(in);
            }
        }

        if (in.readBoolean()) {
            String alias = Text.readString(in);
            aliases_ = new String[] { alias };
        }
    }
}
