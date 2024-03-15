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

package org.apache.doris.catalog.authorizer.ranger.hive;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangerHiveAuditHandler extends RangerDefaultAuditHandler {

    public static final String ACCESS_TYPE_ROWFILTER = "ROW_FILTER";
    public static final String ACCESS_TYPE_INSERT = "INSERT";
    public static final String ACCESS_TYPE_UPDATE = "UPDATE";
    public static final String ACCESS_TYPE_DELETE = "DELETE";
    public static final String ACCESS_TYPE_TRUNCATE = "TRUNCATE";
    public static final String ACTION_TYPE_METADATA_OPERATION = "METADATA OPERATION";
    public static final String CONF_AUDIT_QUERY_REQUEST_SIZE = "xasecure.audit.solr.limit.query.req.size";
    public static final int DEFAULT_CONF_AUDIT_QUERY_REQUEST_SIZE = Integer.MAX_VALUE;
    private static final Logger LOG = LoggerFactory.getLogger(RangerDefaultAuditHandler.class);
    private static final Set<String> ROLE_OPS = new HashSet<>();

    static {
        for (HiveOperationType e : EnumSet.of(HiveOperationType.CREATEROLE, HiveOperationType.DROPROLE,
                HiveOperationType.SHOW_ROLES, HiveOperationType.SHOW_ROLE_GRANT, HiveOperationType.SHOW_ROLE_PRINCIPALS,
                HiveOperationType.GRANT_ROLE, HiveOperationType.REVOKE_ROLE)) {
            ROLE_OPS.add(e.name());
        }
    }

    private final int requestQuerySize;
    private final Collection<AuthzAuditEvent> auditEvents = new ArrayList<>();
    private boolean deniedExists = false;

    public RangerHiveAuditHandler() {
        super();
        requestQuerySize = DEFAULT_CONF_AUDIT_QUERY_REQUEST_SIZE;
    }

    public RangerHiveAuditHandler(Configuration config) {
        super(config);

        int configRequestQuerySize = config.getInt(CONF_AUDIT_QUERY_REQUEST_SIZE,
                DEFAULT_CONF_AUDIT_QUERY_REQUEST_SIZE);

        requestQuerySize = (configRequestQuerySize < 1) ? DEFAULT_CONF_AUDIT_QUERY_REQUEST_SIZE :
            configRequestQuerySize;
    }

    AuthzAuditEvent createAuditEvent(RangerAccessResult result, String accessType, String resourcePath) {
        RangerAccessRequest request = result.getAccessRequest();
        RangerAccessResource resource = request.getResource();
        String resourceType = resource != null ? resource.getLeafName() : null;

        AuthzAuditEvent auditEvent = super.getAuthzEvents(result);

        String resourcePathComputed = resourcePath;
        if (LOG.isDebugEnabled()) {
            LOG.debug("requestQuerySize = " + requestQuerySize);
        }
        if (StringUtils.isNotBlank(request.getRequestData()) && request.getRequestData().length() > requestQuerySize) {
            auditEvent.setRequestData(request.getRequestData().substring(0, requestQuerySize));
        } else {
            auditEvent.setRequestData(request.getRequestData());
        }
        auditEvent.setAccessType(accessType);
        auditEvent.setResourcePath(resourcePathComputed);
        auditEvent.setResourceType("@" + resourceType); // to be consistent with earlier release

        if (request instanceof RangerAccessRequestImpl && resource instanceof RangerHiveResource) {
            RangerAccessRequestImpl hiveAccessRequest = (RangerAccessRequestImpl) request;
            RangerHiveResource hiveResource = (RangerHiveResource) resource;
            String hiveAccessType = hiveAccessRequest.getAccessType();

            if (HiveAccessType.USE.toString().equalsIgnoreCase(hiveAccessType) && hiveResource.getObjectType()
                    == HiveObjectType.DATABASE && StringUtils.isBlank(hiveResource.getDatabase())) {
                // this should happen only for SHOWDATABASES
                auditEvent.setTags(null);
            }
        }

        return auditEvent;
    }

    AuthzAuditEvent createAuditEvent(RangerAccessResult result) {
        final AuthzAuditEvent ret;

        RangerAccessRequest request = result.getAccessRequest();
        RangerAccessResource resource = request.getResource();
        String resourcePath = resource != null ? resource.getAsString() : null;
        int policyType = result.getPolicyType();

        if (policyType == RangerPolicy.POLICY_TYPE_DATAMASK && result.isMaskEnabled()) {
            ret = createAuditEvent(result, result.getMaskType(), resourcePath);
        } else if (policyType == RangerPolicy.POLICY_TYPE_ROWFILTER) {
            ret = createAuditEvent(result, ACCESS_TYPE_ROWFILTER, resourcePath);
        } else if (policyType == RangerPolicy.POLICY_TYPE_ACCESS) {
            String accessType = null;

            if (request instanceof RangerAccessRequestImpl) {
                RangerAccessRequestImpl hiveRequest = (RangerAccessRequestImpl) request;

                accessType = hiveRequest.getAccessType();

                String action = request.getAction();
                if (ACTION_TYPE_METADATA_OPERATION.equals(action)) {
                    accessType = ACTION_TYPE_METADATA_OPERATION;
                } else if (HiveAccessType.UPDATE.toString().equalsIgnoreCase(accessType)) {
                    String commandStr = request.getRequestData();
                    if (StringUtils.isNotBlank(commandStr)) {
                        if (StringUtils.startsWithIgnoreCase(commandStr, ACCESS_TYPE_INSERT)) {
                            accessType = ACCESS_TYPE_INSERT;
                        } else if (StringUtils.startsWithIgnoreCase(commandStr, ACCESS_TYPE_UPDATE)) {
                            accessType = ACCESS_TYPE_UPDATE;
                        } else if (StringUtils.startsWithIgnoreCase(commandStr, ACCESS_TYPE_DELETE)) {
                            accessType = ACCESS_TYPE_DELETE;
                        } else if (StringUtils.startsWithIgnoreCase(commandStr, ACCESS_TYPE_TRUNCATE)) {
                            accessType = ACCESS_TYPE_TRUNCATE;
                        }
                    }
                }
            }

            if (StringUtils.isEmpty(accessType)) {
                accessType = request.getAccessType();
            }

            ret = createAuditEvent(result, accessType, resourcePath);
        } else {
            ret = null;
        }

        return ret;
    }

    List<AuthzAuditEvent> createAuditEvents(Collection<RangerAccessResult> results) {

        Map<Long, AuthzAuditEvent> auditEventsMap = new HashMap<>();
        Iterator<RangerAccessResult> iterator = results.iterator();
        AuthzAuditEvent deniedAuditEvent = null;
        while (iterator.hasNext() && deniedAuditEvent == null) {
            RangerAccessResult result = iterator.next();
            if (result.getIsAudited()) {
                if (!result.getIsAllowed()) {
                    deniedAuditEvent = createAuditEvent(result);
                } else {
                    long policyId = result.getPolicyId();
                    // add this result to existing event by updating column values
                    if (auditEventsMap.containsKey(policyId)) {
                        AuthzAuditEvent auditEvent = auditEventsMap.get(policyId);
                        RangerAccessRequestImpl request = (RangerAccessRequestImpl) result.getAccessRequest();
                        RangerHiveResource resource = (RangerHiveResource) request.getResource();
                        String resourcePath = auditEvent.getResourcePath() + "," + resource.getColumn();
                        auditEvent.setResourcePath(resourcePath);
                        Set<String> tags = getTags(request);
                        if (tags != null) {
                            auditEvent.getTags().addAll(tags);
                        }
                    } else { // new event as this approval was due to a different policy.
                        AuthzAuditEvent auditEvent = createAuditEvent(result);

                        if (auditEvent != null) {
                            auditEventsMap.put(policyId, auditEvent);
                        }
                    }
                }
            }
        }
        final List<AuthzAuditEvent> result = (deniedAuditEvent == null) ? new ArrayList<>(auditEventsMap.values())
                : Collections.singletonList(deniedAuditEvent);

        return result;
    }

    @Override
    public void processResult(RangerAccessResult result) {
        if (result == null || !result.getIsAudited()) {
            return;
        }

        if (skipFilterOperationAuditing(result)) {
            return;
        }

        AuthzAuditEvent auditEvent = createAuditEvent(result);

        if (auditEvent != null) {
            addAuthzAuditEvent(auditEvent);
        }
    }

    /**
     * This method is expected to be called ONLY to process the results for multiple-columns in a table.
     * To ensure this, RangerHiveAccessController should call isAccessAllowed(Collection<requests>) only for this
     * condition
     */
    @Override
    public void processResults(Collection<RangerAccessResult> results) {
        List<AuthzAuditEvent> result = createAuditEvents(results);
        for (AuthzAuditEvent auditEvent : result) {
            addAuthzAuditEvent(auditEvent);
        }
    }

    public void flushAudit() {
        for (AuthzAuditEvent auditEvent : auditEvents) {
            if (deniedExists && auditEvent.getAccessResult() != 0) { // if deny exists, skip logging for allowed results
                continue;
            }

            super.logAuthzAudit(auditEvent);
        }
    }

    private void addAuthzAuditEvent(AuthzAuditEvent auditEvent) {
        if (auditEvent != null) {
            auditEvents.add(auditEvent);

            if (auditEvent.getAccessResult() == 0) {
                deniedExists = true;
            }
        }
    }

    private boolean skipFilterOperationAuditing(RangerAccessResult result) {
        boolean ret = false;
        RangerAccessRequest accessRequest = result.getAccessRequest();
        if (accessRequest != null) {
            String action = accessRequest.getAction();
            if (ACTION_TYPE_METADATA_OPERATION.equals(action) && !result.getIsAllowed()) {
                ret = true;
            }
        }
        return ret;
    }
}
