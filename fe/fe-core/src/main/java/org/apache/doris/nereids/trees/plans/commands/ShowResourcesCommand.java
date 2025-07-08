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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * show resources command
 */
public class ShowResourcesCommand extends ShowCommand {
    // RESOURCE_PROC_NODE_TITLE_NAMES copy from org.apache.doris.catalog.ResourceMgr
    public static final ImmutableList<String> RESOURCE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("ResourceType").add("Item").add("Value")
            .build();

    private final Expression wildWhere;
    private String likePattern;
    private final List<OrderKey> orderKeys;
    private final long limit;
    private final long offset;
    private boolean isAccurateMatch;
    private String nameValue;
    private String typeValue;
    private ArrayList<OrderByPair> orderByPairs;

    /**
     * constructor for show resources
     */
    public ShowResourcesCommand(Expression wildWhere, String likePattern, List<OrderKey> orderKeys,
                                    long limit, long offset) {
        super(PlanType.SHOW_RESOURCES_COMMAND);
        this.wildWhere = wildWhere;
        this.likePattern = likePattern;
        this.orderKeys = orderKeys;
        this.limit = limit;
        this.offset = offset;
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : RESOURCE_PROC_NODE_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    public boolean isAccurateMatch() {
        return this.isAccurateMatch;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    /**
     * handle show resources
     */
    private ShowResultSet handleShowResources(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // first validate the auth and where
        validate(ctx);

        // then process the order by
        orderByPairs = getOrderByPairs(orderKeys, RESOURCE_PROC_NODE_TITLE_NAMES);

        // when execute "show resources like '%xxx%'", the likePattern is '%xxx%',
        // when execute "shore resources where name like '%xxx%'", the likePattern is null,
        // we use likePattern to handle the two statements.
        PatternMatcher matcher = null;
        if (likePattern != null) {
            matcher = PatternMatcherWrapper.createMysqlPattern(likePattern,
                    CaseSensibility.RESOURCE.getCaseSensibility());
        } else {
            if (wildWhere instanceof Like) {
                if (wildWhere.child(1) instanceof StringLikeLiteral) {
                    likePattern = ((StringLikeLiteral) wildWhere.child(1)).getStringValue();
                    matcher = PatternMatcherWrapper.createMysqlPattern(likePattern,
                            CaseSensibility.RESOURCE.getCaseSensibility());
                    nameValue = null;
                }
            }
        }

        // sort the result
        List<List<Comparable>> resourcesInfos = Env.getCurrentEnv().getResourceMgr()
                .getResourcesInfo(matcher, nameValue, isAccurateMatch, getTypeSet());
        ListComparator<List<Comparable>> comparator = null;
        if (orderByPairs != null) {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<List<Comparable>>(orderByPairs.toArray(orderByPairArr));
        } else {
            // sort by name asc
            comparator = new ListComparator<List<Comparable>>(0);
        }
        Collections.sort(resourcesInfos, comparator);

        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> resourceInfo : resourcesInfos) {
            List<String> oneResource = new ArrayList<String>(resourceInfo.size());

            for (Comparable element : resourceInfo) {
                oneResource.add(element.toString());
            }
            rows.add(oneResource);
        }

        // limit and offset
        rows = applyLimit(limit, offset, rows);

        // Only success
        return new ShowResultSet(getMetaData(), rows);
    }

    /**
     * validate the auth and where
     */
    @VisibleForTesting
    protected boolean validate(ConnectContext ctx) throws UserException {
        boolean isValid = true;
        if (this.likePattern == null) {
            if (wildWhere instanceof CompoundPredicate) {
                if (!(wildWhere instanceof And)) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }
                isValid = isWhereClauseValid(wildWhere.child(0)) && isWhereClauseValid(wildWhere.child(1));
            } else {
                isValid = isWhereClauseValid(wildWhere);
            }

            if (!isValid) {
                throw new AnalysisException("Where clause should looks like: NAME = \"your_resource_name\","
                    + " or NAME LIKE \"matcher\", " + " or RESOURCETYPE = \"resource_type\", "
                    + " or compound predicate with operator AND");
            }
        }

        return true;
    }

    private Set<String> getTypeSet() {
        if (Strings.isNullOrEmpty(typeValue)) {
            return null;
        }
        Set<String> resources = new HashSet<>();
        resources.add(typeValue.toUpperCase());
        return resources;
    }

    private boolean isWhereClauseValid(Expression expr) {
        if (expr == null) {
            return true;
        }

        if (!(expr instanceof EqualTo) && !(expr instanceof Like)) {
            return false;
        }

        // left child
        if (!(expr.child(0) instanceof UnboundSlot)) {
            return false;
        }
        String leftKey = ((UnboundSlot) expr.child(0)).getName();
        // right child
        if (!(expr.child(1) instanceof StringLikeLiteral)) {
            return false;
        }
        String rightValue = ((StringLikeLiteral) expr.child(1)).getStringValue();
        if (Strings.isNullOrEmpty(rightValue)) {
            return false;
        }

        if (leftKey.equalsIgnoreCase("Name")) {
            if (expr instanceof EqualTo) {
                isAccurateMatch = true;
            }
            nameValue = rightValue;
            return true;
        }

        if (leftKey.equalsIgnoreCase("ResourceType") && expr instanceof EqualTo) {
            typeValue = rightValue.toUpperCase();
            try {
                Resource.ResourceType.valueOf(typeValue);
            } catch (Exception e) {
                return false;
            }
            return true;
        }

        return false;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowResources(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowResourcesCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        if (ConnectContext.get().getSessionVariable().getForwardToMaster()) {
            return RedirectStatus.FORWARD_NO_SYNC;
        } else {
            return RedirectStatus.NO_FORWARD;
        }
    }
}
