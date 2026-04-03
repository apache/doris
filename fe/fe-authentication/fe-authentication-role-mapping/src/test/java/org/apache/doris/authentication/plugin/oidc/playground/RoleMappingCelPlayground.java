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

package org.apache.doris.authentication.plugin.oidc.playground;

import org.apache.doris.authentication.BasicPrincipal;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelValidationResult;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Temporary playground for trying CEL role-mapping expressions against the
 * current OIDC-to-Principal shape in Doris.
 */
public final class RoleMappingCelPlayground {

    private static final CelCompiler COMPILER = CelCompilerFactory.standardCelCompilerBuilder()
            .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
            .addVar("rawClaims", SimpleType.DYN)
            .addVar("principalView", SimpleType.DYN)
            .build();

    private static final CelRuntime RUNTIME = CelRuntimeFactory.standardCelRuntimeBuilder()
            .build();

    private RoleMappingCelPlayground() {
    }

    public static void main(String[] args) throws Exception {
        Map<String, Object> rawClaims = sampleRawClaims();
        Map<String, Object> principalView = samplePrincipalView();
        List<String> expressions = args.length == 0 ? defaultExpressions() : Arrays.asList(args);

        System.out.println("=== rawClaims ===");
        System.out.println(rawClaims);
        System.out.println();

        System.out.println("=== principalView ===");
        System.out.println(principalView);
        System.out.println();

        System.out.println("=== notes ===");
        System.out.println("- rawClaims preserves nested maps/lists like realm_access.roles");
        System.out.println("- principalView mirrors the current Doris Principal shape");
        System.out.println("- principalView.attributes is Map<String, String>; nested claims are already flattened/lost");
        System.out.println();

        System.out.println("=== expressions ===");
        for (String expression : expressions) {
            Object result = evaluate(expression, rawClaims, principalView);
            System.out.println(expression + " => " + result);
        }
    }

    public static Object evaluate(String expression) throws Exception {
        return evaluate(expression, sampleRawClaims(), samplePrincipalView());
    }

    static Object evaluate(String expression, Map<String, Object> rawClaims, Map<String, Object> principalView)
            throws Exception {
        CelValidationResult validationResult = COMPILER.compile(expression);
        if (validationResult.hasError()) {
            throw new IllegalArgumentException(validationResult.getErrorString());
        }

        CelAbstractSyntaxTree ast = validationResult.getAst();
        return RUNTIME.createProgram(ast).eval(activation(rawClaims, principalView));
    }

    static Map<String, Object> sampleRawClaims() {
        Map<String, Object> realmAccess = new LinkedHashMap<>();
        realmAccess.put("roles", Arrays.asList("db_admin", "monitoring"));

        Map<String, Object> resourceAccessDoris = new LinkedHashMap<>();
        resourceAccessDoris.put("roles", Arrays.asList("query_runner", "dashboard_viewer"));

        Map<String, Object> resourceAccess = new LinkedHashMap<>();
        resourceAccess.put("doris", resourceAccessDoris);

        Map<String, Object> rawClaims = new LinkedHashMap<>();
        rawClaims.put("preferred_username", "alice");
        rawClaims.put("sub", "oidc:alice");
        rawClaims.put("email", "alice@example.com");
        rawClaims.put("tenant", "acme");
        rawClaims.put("groups", Arrays.asList("analytics", "doris_users"));
        rawClaims.put("realm_access", realmAccess);
        rawClaims.put("resource_access", resourceAccess);
        return rawClaims;
    }

    static Map<String, Object> samplePrincipalView() {
        BasicPrincipal principal = BasicPrincipal.builder()
                .name("alice")
                .authenticator("oidc_primary")
                .externalPrincipal("oidc:alice")
                .externalGroups(new LinkedHashSet<>(Arrays.asList("analytics", "doris_users")))
                .attribute("email", "alice@example.com")
                .attribute("tenant", "acme")
                .build();

        Map<String, Object> principalView = new LinkedHashMap<>();
        principalView.put("name", principal.getName());
        principalView.put("authenticator", principal.getAuthenticator());
        principalView.put("externalPrincipal", principal.getExternalPrincipal().orElse(null));
        principalView.put("externalGroups", new ArrayList<>(principal.getExternalGroups()));
        principalView.put("attributes", new LinkedHashMap<>(principal.getAttributes()));
        return principalView;
    }

    private static Map<String, Object> activation(Map<String, Object> rawClaims, Map<String, Object> principalView) {
        Map<String, Object> activation = new LinkedHashMap<>();
        activation.put("rawClaims", rawClaims);
        activation.put("principalView", principalView);
        return activation;
    }

    private static List<String> defaultExpressions() {
        List<String> expressions = new ArrayList<>();
        expressions.add("rawClaims['groups'].exists(g, g == 'analytics')");
        expressions.add("rawClaims['realm_access']['roles'].exists(r, r == 'db_admin')");
        expressions.add("principalView['externalGroups'].exists(g, g == 'analytics')");
        expressions.add("principalView['attributes']['tenant'] == 'acme'");
        return expressions;
    }
}
