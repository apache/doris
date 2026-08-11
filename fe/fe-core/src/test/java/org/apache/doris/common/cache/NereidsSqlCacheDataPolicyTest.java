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

package org.apache.doris.common.cache;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.DataMaskPolicy;
import org.apache.doris.mysql.privilege.RangerDataMaskPolicy;
import org.apache.doris.mysql.privilege.RangerRowFilterPolicy;
import org.apache.doris.mysql.privilege.RowFilterPolicy;
import org.apache.doris.nereids.SqlCacheContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

/**
 * Before serving a cached result, {@link NereidsSqlCacheManager} re-evaluates the row-filter and data-mask
 * policies of every table the query touched and compares them with what was recorded at plan time. The
 * comparison is by value ({@code isEqualCollection} / {@code Objects.equals}), so it only answers the
 * intended question - "has the administrator changed a policy?" - if an unchanged policy re-evaluates to an
 * equal object.
 *
 * <p>An authorization source builds its policy objects fresh on every evaluation (it is answering a request,
 * not handing out a cached object), so value equality is the source's obligation, and the "unchanged" tests
 * below are what enforce it. They are RED for the Ranger policy types, which have no equals: the comparison
 * degrades to identity, every lookup reads as a policy change, and a user with any row filter or column mask
 * configured never gets a cache hit at all.</p>
 *
 * <p>The "changed" tests pin the other half of the contract, so a fix cannot buy cache hits by weakening the
 * comparison: an edited policy must still invalidate, or the cache keeps serving rows the user may no longer
 * see.</p>
 */
public class NereidsSqlCacheDataPolicyTest {

    private static final String CTL = "internal";
    private static final String DB = "test_db";
    private static final String TBL = "orders";
    private static final String COL = "phone";
    private static final UserIdentity USER = UserIdentity.ROOT;

    private RowFilterPolicy rowFilter(long policyVersion, String filterExpr) {
        return new RangerRowFilterPolicy(USER, CTL, DB, TBL, 7L, policyVersion, filterExpr);
    }

    private DataMaskPolicy dataMask(long policyVersion, String maskTypeDef) {
        return new RangerDataMaskPolicy(USER, CTL, DB, TBL, COL, 7L, policyVersion, "CUSTOM", maskTypeDef);
    }

    /**
     * Wires an {@link Env} whose access manager re-evaluates to the supplied policies. The supplier is
     * invoked per call so every evaluation yields a distinct object, exactly as a live authorization source
     * behaves.
     */
    private Env mockEnvEvaluating(java.util.function.Supplier<RowFilterPolicy> rowFilterSupplier,
            java.util.function.Supplier<Optional<DataMaskPolicy>> dataMaskSupplier) {
        AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
        Mockito.doAnswer(invocation -> {
            RowFilterPolicy evaluated = rowFilterSupplier.get();
            return evaluated == null ? ImmutableList.of() : ImmutableList.of(evaluated);
        })
                .when(accessManager).evalRowFilterPolicies(ArgumentMatchers.any(UserIdentity.class),
                        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString());
        Mockito.doAnswer(invocation -> dataMaskSupplier.get())
                .when(accessManager).evalDataMaskPolicy(ArgumentMatchers.any(UserIdentity.class),
                        ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString());

        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getAccessManager()).thenReturn(accessManager);
        return env;
    }

    private boolean rowPoliciesChanged(Env env, SqlCacheContext context) {
        return Deencapsulation.invoke(new NereidsSqlCacheManager(), "rowPoliciesChanged", USER, env, context);
    }

    private boolean dataMaskPoliciesChanged(Env env, SqlCacheContext context) {
        return Deencapsulation.invoke(new NereidsSqlCacheManager(), "dataMaskPoliciesChanged", USER, env, context);
    }

    private SqlCacheContext contextWithRowFilter(RowFilterPolicy policy) {
        SqlCacheContext context = new SqlCacheContext(USER);
        List<RowFilterPolicy> policies = policy == null ? ImmutableList.of() : ImmutableList.of(policy);
        context.setRowFilterPolicy(CTL, DB, TBL, policies);
        return context;
    }

    private SqlCacheContext contextWithDataMask(Optional<DataMaskPolicy> policy) {
        SqlCacheContext context = new SqlCacheContext(USER);
        context.addDataMaskPolicy(CTL, DB, TBL, COL, policy);
        return context;
    }

    /** The administrator touched nothing: the same filter must re-evaluate equal and keep the cache. */
    @Test
    public void testUntouchedRowFilterKeepsCache() {
        SqlCacheContext context = contextWithRowFilter(rowFilter(3L, "region = 'cn'"));
        Env env = mockEnvEvaluating(() -> rowFilter(3L, "region = 'cn'"), Optional::empty);

        Assertions.assertFalse(rowPoliciesChanged(env, context),
                "an unchanged row filter must not read as a policy change, otherwise every cache lookup for a "
                        + "user with row-level security evicts the entry and the SQL cache never serves anything");
    }

    /** The administrator edited the policy: the cache must not serve rows filtered by the old predicate. */
    @Test
    public void testEditedRowFilterInvalidatesCache() {
        SqlCacheContext context = contextWithRowFilter(rowFilter(3L, "region = 'cn'"));
        Env env = mockEnvEvaluating(() -> rowFilter(4L, "region IN ('cn', 'us')"), Optional::empty);

        Assertions.assertTrue(rowPoliciesChanged(env, context),
                "an edited row filter must invalidate the cache");
    }

    /** A revoked policy widens nothing but still changes the result set, so it must invalidate too. */
    @Test
    public void testRemovedRowFilterInvalidatesCache() {
        SqlCacheContext context = contextWithRowFilter(rowFilter(3L, "region = 'cn'"));
        Env env = mockEnvEvaluating(() -> null, Optional::empty);

        Assertions.assertTrue(rowPoliciesChanged(env, context),
                "dropping a row filter changes which rows the query returns");
    }

    /** Same contract for column masking. */
    @Test
    public void testUntouchedDataMaskKeepsCache() {
        SqlCacheContext context = contextWithDataMask(Optional.of(dataMask(3L, "CONCAT('XXXX', SUBSTR(phone, -4))")));
        Env env = mockEnvEvaluating(() -> null,
                () -> Optional.of(dataMask(3L, "CONCAT('XXXX', SUBSTR(phone, -4))")));

        Assertions.assertFalse(dataMaskPoliciesChanged(env, context),
                "an unchanged column mask must not read as a policy change, otherwise every cache lookup for a "
                        + "user with a masked column evicts the entry");
    }

    @Test
    public void testEditedDataMaskInvalidatesCache() {
        SqlCacheContext context = contextWithDataMask(Optional.of(dataMask(3L, "CONCAT('XXXX', SUBSTR(phone, -4))")));
        Env env = mockEnvEvaluating(() -> null, () -> Optional.of(dataMask(4L, "NULL")));

        Assertions.assertTrue(dataMaskPoliciesChanged(env, context),
                "a strengthened column mask must invalidate the cache");
    }

    /** Unmasking a column exposes raw values: the cached masked result is stale, and so is the reverse. */
    @Test
    public void testRemovedDataMaskInvalidatesCache() {
        SqlCacheContext context = contextWithDataMask(Optional.of(dataMask(3L, "NULL")));
        Env env = mockEnvEvaluating(() -> null, Optional::empty);

        Assertions.assertTrue(dataMaskPoliciesChanged(env, context),
                "dropping a column mask changes the values the query returns");
    }

    /** A column that never had a mask must not keep evicting the cache either. */
    @Test
    public void testColumnWithoutMaskKeepsCache() {
        SqlCacheContext context = contextWithDataMask(Optional.empty());
        Env env = mockEnvEvaluating(() -> null, Optional::empty);

        Assertions.assertFalse(dataMaskPoliciesChanged(env, context),
                "an unmasked column must not read as a policy change");
    }
}
