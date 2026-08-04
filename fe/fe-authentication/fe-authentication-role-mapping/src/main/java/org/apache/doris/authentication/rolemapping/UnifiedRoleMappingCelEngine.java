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

package org.apache.doris.authentication.rolemapping;

import org.apache.doris.authentication.BasicPrincipal;
import org.apache.doris.authentication.Principal;

import dev.cel.common.CelAbstractSyntaxTree;
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.CelValidationResult;
import dev.cel.common.types.CelType;
import dev.cel.common.types.SimpleType;
import dev.cel.compiler.CelCompiler;
import dev.cel.compiler.CelCompilerFactory;
import dev.cel.parser.CelStandardMacro;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelRuntime;
import dev.cel.runtime.CelRuntimeFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Single-file prototype of a unified CEL-based role-mapping engine.
 *
 * <p>This intentionally does not depend on OIDC token parsing or FE session
 * wiring. It only implements the core rule compilation and evaluation model
 * described by the unified role-mapping design.</p>
 */
public final class UnifiedRoleMappingCelEngine {

    private static final int MAX_VARIADIC_ARGUMENTS = 8;
    private static final ThreadLocal<EvaluationContext> CURRENT_CONTEXT = new ThreadLocal<>();
    private static final CelCompiler COMPILER = createCompiler();
    private static final CelRuntime RUNTIME = createRuntime();

    private final List<CompiledRule> compiledRules;

    public UnifiedRoleMappingCelEngine(List<Rule> rules) {
        Objects.requireNonNull(rules, "rules is required");
        List<CompiledRule> newCompiledRules = new ArrayList<>(rules.size());
        for (Rule rule : rules) {
            newCompiledRules.add(compileRule(rule));
        }
        this.compiledRules = Collections.unmodifiableList(newCompiledRules);
    }

    public Set<String> evaluate(EvaluationContext context) {
        Objects.requireNonNull(context, "context is required");
        LinkedHashSet<String> grantedRoles = new LinkedHashSet<>();
        CURRENT_CONTEXT.set(context);
        try {
            for (CompiledRule rule : compiledRules) {
                Object result = rule.program.eval(Collections.emptyMap());
                if (!(result instanceof Boolean)) {
                    throw new IllegalStateException("Role mapping condition must evaluate to boolean");
                }
                if ((Boolean) result) {
                    grantedRoles.addAll(rule.grantedRoles);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to evaluate role mapping rules", e);
        } finally {
            CURRENT_CONTEXT.remove();
        }
        return Collections.unmodifiableSet(grantedRoles);
    }

    public static void main(String[] args) {
        DemoResult demoResult = runMultipleMatchDemo();
        System.out.println("=== unified role mapping demo ===");
        System.out.println("principal: " + demoResult.getContext().getName());
        System.out.println("external principal: " + demoResult.getContext().getExternalPrincipal());
        System.out.println("rules:");
        for (Rule rule : demoResult.getRules()) {
            System.out.println("  " + rule.getCondition() + " => " + rule.getGrantedRoles());
        }
        System.out.println("final roles: " + demoResult.getFinalRoles());
    }

    public static DemoResult runMultipleMatchDemo() {
        List<Rule> rules = List.of(
                Rule.of("has_group(\"team-a\")", Set.of("team_a_logs_reader")),
                Rule.of("has_group(\"oncall\") && has_scope(\"logs:write\")", Set.of("oncall_logs_writer")),
                Rule.of("has_role(\"incident-admin\")", Set.of("incident_admin", "security_audit_reader")),
                Rule.of("has_scope(\"session:role:analyst\")", Set.of("analyst", "dashboard_readonly")),
                Rule.of("attr(\"environment\") == \"prod\"", Set.of("prod_observer")),
                Rule.of("is_service_principal() && has_scope(\"ingest:write\")", Set.of("ingest_writer")));
        UnifiedRoleMappingCelEngine engine = new UnifiedRoleMappingCelEngine(rules);
        Principal principal = BasicPrincipal.builder()
                .name("alice")
                .externalPrincipal("oidc:alice")
                .externalGroups(Set.of("team-a", "oncall"))
                .attributes(Map.of("environment", "prod"))
                .authenticator("oidc_demo")
                .build();
        EvaluationContext context = fromPrincipal(principal, Map.of(
                "roles", Set.of("incident-admin"),
                "scope", Set.of("logs:write"),
                "scp", Set.of("session:role:analyst")));
        return new DemoResult(rules, context, engine.evaluate(context));
    }

    public static EvaluationContext fromPrincipal(Principal principal) {
        return fromPrincipal(principal, principal.getMultiValueAttributes());
    }

    public static EvaluationContext fromPrincipal(
            Principal principal, Map<String, Set<String>> multiValueAttributes) {
        Objects.requireNonNull(principal, "principal is required");
        return EvaluationContext.builder()
                .name(principal.getName())
                .externalPrincipal(principal.getExternalPrincipal().orElse(""))
                .externalGroups(principal.getExternalGroups())
                .attributes(principal.getAttributes())
                .multiValueAttributes(multiValueAttributes)
                .servicePrincipal(principal.isServicePrincipal())
                .build();
    }

    private static CompiledRule compileRule(Rule rule) {
        Objects.requireNonNull(rule, "rule is required");
        CelValidationResult validationResult = COMPILER.compile(rule.condition);
        if (validationResult.hasError()) {
            throw new IllegalArgumentException(
                    "Invalid CEL expression '" + rule.condition + "': " + validationResult.getErrorString());
        }
        try {
            CelAbstractSyntaxTree ast = validationResult.getAst();
            return new CompiledRule(rule, RUNTIME.createProgram(ast));
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to create CEL program for expression '" + rule.condition + "'",
                    e);
        }
    }

    private static CelCompiler createCompiler() {
        return CelCompilerFactory.standardCelCompilerBuilder()
                .setStandardMacros(CelStandardMacro.STANDARD_MACROS)
                .setResultType(SimpleType.BOOL)
                .addFunctionDeclarations(createFunctionDeclarations())
                .build();
    }

    private static CelRuntime createRuntime() {
        return CelRuntimeFactory.standardCelRuntimeBuilder()
                .addFunctionBindings(createFunctionBindings())
                .build();
    }

    private static List<CelFunctionDecl> createFunctionDeclarations() {
        List<CelFunctionDecl> declarations = new ArrayList<>();
        declarations.add(function("name", overload("name_string", SimpleType.STRING)));
        declarations.add(function("external_principal", overload("external_principal_string", SimpleType.STRING)));
        declarations.add(function("is_service_principal", overload("is_service_principal_bool", SimpleType.BOOL)));
        declarations.add(function("has_group", overload("has_group_string", SimpleType.BOOL, SimpleType.STRING)));
        declarations.add(function("has_role", overload("has_role_string", SimpleType.BOOL, SimpleType.STRING)));
        declarations.add(function("has_scope", overload("has_scope_string", SimpleType.BOOL, SimpleType.STRING)));
        declarations.add(function("attr", overload("attr_string", SimpleType.STRING, SimpleType.STRING)));
        declarations.add(function("has_attr_value", overload(
                "has_attr_value_string_string", SimpleType.BOOL, SimpleType.STRING, SimpleType.STRING)));
        declarations.add(function("has_any_group", variadicOverloads("has_any_group", SimpleType.BOOL, 1)));
        declarations.add(function("has_any_role", variadicOverloads("has_any_role", SimpleType.BOOL, 1)));
        declarations.add(function("has_any_scope", variadicOverloads("has_any_scope", SimpleType.BOOL, 1)));
        declarations.add(function("has_any_attr_value", variadicOverloads("has_any_attr_value", SimpleType.BOOL, 2)));
        return declarations;
    }

    private static List<CelFunctionBinding> createFunctionBindings() {
        List<CelFunctionBinding> bindings = new ArrayList<>();
        bindings.add(binding("name_string", arguments -> currentContext().name));
        bindings.add(binding("external_principal_string", arguments -> currentContext().externalPrincipal));
        bindings.add(binding("is_service_principal_bool", arguments -> currentContext().servicePrincipal));
        bindings.add(binding("has_group_string", String.class,
                arguments -> currentContext().externalGroups.contains(arguments)));
        bindings.add(binding("has_role_string", String.class,
                arguments -> currentContext().multiValueContains("roles", arguments)));
        bindings.add(binding("has_scope_string", String.class,
                arguments -> currentContext().hasScope(arguments)));
        bindings.add(binding("attr_string", String.class,
                arguments -> currentContext().attributes.getOrDefault(arguments, "")));
        bindings.add(binding("has_attr_value_string_string", String.class, String.class,
                (key, value) -> currentContext().hasAttributeValue(key, value)));
        addVariadicBindings(bindings, "has_any_group", 1, arguments -> currentContext().externalGroups
                .stream().anyMatch(arguments::contains));
        addVariadicBindings(bindings, "has_any_role", 1, arguments -> arguments.stream()
                .anyMatch(candidate -> currentContext().multiValueContains("roles", candidate)));
        addVariadicBindings(bindings, "has_any_scope", 1, arguments -> arguments.stream()
                .anyMatch(candidate -> currentContext().hasScope(candidate)));
        addVariadicBindings(bindings, "has_any_attr_value", 2, arguments -> {
            String key = arguments.get(0);
            List<String> values = arguments.subList(1, arguments.size());
            return currentContext().hasAnyAttributeValue(key, values);
        });
        return bindings;
    }

    private static CelFunctionDecl function(String name, CelOverloadDecl... overloads) {
        return CelFunctionDecl.newFunctionDeclaration(name, overloads);
    }

    private static CelFunctionDecl function(String name, List<CelOverloadDecl> overloads) {
        return CelFunctionDecl.newFunctionDeclaration(name, overloads);
    }

    private static CelOverloadDecl overload(String overloadId, CelType resultType, CelType... parameterTypes) {
        return CelOverloadDecl.newGlobalOverload(overloadId, resultType, parameterTypes);
    }

    private static List<CelOverloadDecl> variadicOverloads(String functionName, CelType resultType, int minArgs) {
        List<CelOverloadDecl> overloads = new ArrayList<>();
        for (int argCount = minArgs; argCount <= MAX_VARIADIC_ARGUMENTS; argCount++) {
            CelType[] parameterTypes = new CelType[argCount];
            for (int i = 0; i < argCount; i++) {
                parameterTypes[i] = SimpleType.STRING;
            }
            overloads.add(CelOverloadDecl.newGlobalOverload(
                    functionName + "_strings_" + argCount, resultType, parameterTypes));
        }
        return overloads;
    }

    private static CelFunctionBinding binding(String overloadId, BindingFunction function) {
        return CelFunctionBinding.from(overloadId, List.of(), arguments -> function.apply(arguments));
    }

    private static <T> CelFunctionBinding binding(
            String overloadId, Class<T> argType, UnaryBindingFunction<T> function) {
        return CelFunctionBinding.from(overloadId, argType, function::apply);
    }

    private static <T1, T2> CelFunctionBinding binding(
            String overloadId,
            Class<T1> firstArgType,
            Class<T2> secondArgType,
            BinaryBindingFunction<T1, T2> function) {
        return CelFunctionBinding.from(overloadId, firstArgType, secondArgType, function::apply);
    }

    private static void addVariadicBindings(
            List<CelFunctionBinding> bindings, String functionName, int minArgs, VariadicBindingFunction function) {
        for (int argCount = minArgs; argCount <= MAX_VARIADIC_ARGUMENTS; argCount++) {
            List<Class<?>> parameterTypes = new ArrayList<>(argCount);
            for (int i = 0; i < argCount; i++) {
                parameterTypes.add(String.class);
            }
            String overloadId = functionName + "_strings_" + argCount;
            bindings.add(CelFunctionBinding.from(overloadId, parameterTypes, arguments -> {
                List<String> values = new ArrayList<>(arguments.length);
                for (Object argument : arguments) {
                    values.add((String) argument);
                }
                return function.apply(values);
            }));
        }
    }

    private static EvaluationContext currentContext() {
        EvaluationContext context = CURRENT_CONTEXT.get();
        if (context == null) {
            throw new IllegalStateException("Role mapping helper invoked without evaluation context");
        }
        return context;
    }

    @FunctionalInterface
    private interface BindingFunction {
        Object apply(Object[] arguments);
    }

    @FunctionalInterface
    private interface UnaryBindingFunction<T> {
        Object apply(T argument);
    }

    @FunctionalInterface
    private interface BinaryBindingFunction<T1, T2> {
        Object apply(T1 firstArgument, T2 secondArgument);
    }

    @FunctionalInterface
    private interface VariadicBindingFunction {
        Object apply(List<String> arguments);
    }

    private static final class CompiledRule {
        private final Rule rule;
        private final CelRuntime.Program program;
        private final Set<String> grantedRoles;

        private CompiledRule(Rule rule, CelRuntime.Program program) {
            this.rule = rule;
            this.program = program;
            this.grantedRoles = rule.grantedRoles;
        }
    }

    public static final class DemoResult {
        private final List<Rule> rules;
        private final EvaluationContext context;
        private final Set<String> finalRoles;

        private DemoResult(List<Rule> rules, EvaluationContext context, Set<String> finalRoles) {
            this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
            this.context = context;
            this.finalRoles = Collections.unmodifiableSet(new LinkedHashSet<>(finalRoles));
        }

        public List<Rule> getRules() {
            return rules;
        }

        public EvaluationContext getContext() {
            return context;
        }

        public Set<String> getFinalRoles() {
            return finalRoles;
        }
    }

    public static final class Rule {
        private final String condition;
        private final Set<String> grantedRoles;

        private Rule(String condition, Set<String> grantedRoles) {
            this.condition = Objects.requireNonNull(condition, "condition is required");
            this.grantedRoles = Collections.unmodifiableSet(new LinkedHashSet<>(
                    Objects.requireNonNull(grantedRoles, "grantedRoles is required")));
        }

        public static Rule of(String condition, Set<String> grantedRoles) {
            return new Rule(condition, grantedRoles);
        }

        public String getCondition() {
            return condition;
        }

        public Set<String> getGrantedRoles() {
            return grantedRoles;
        }
    }

    public static final class EvaluationContext {
        private final String name;
        private final String externalPrincipal;
        private final Set<String> externalGroups;
        private final Map<String, String> attributes;
        private final Map<String, Set<String>> multiValueAttributes;
        private final boolean servicePrincipal;

        private EvaluationContext(Builder builder) {
            this.name = builder.name;
            this.externalPrincipal = builder.externalPrincipal != null ? builder.externalPrincipal : "";
            this.externalGroups = Collections.unmodifiableSet(new LinkedHashSet<>(builder.externalGroups));
            this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(builder.attributes));
            this.multiValueAttributes = Collections.unmodifiableMap(
                    copyMultiValueAttributes(builder.multiValueAttributes));
            this.servicePrincipal = builder.servicePrincipal;
        }

        public static Builder builder() {
            return new Builder();
        }

        public String getName() {
            return name;
        }

        public String getExternalPrincipal() {
            return externalPrincipal;
        }

        public Set<String> getExternalGroups() {
            return externalGroups;
        }

        public Map<String, String> getAttributes() {
            return attributes;
        }

        public Map<String, Set<String>> getMultiValueAttributes() {
            return multiValueAttributes;
        }

        public boolean isServicePrincipal() {
            return servicePrincipal;
        }

        private boolean multiValueContains(String key, String candidate) {
            return multiValueAttributes.getOrDefault(key, Collections.emptySet()).contains(candidate);
        }

        private boolean hasScope(String candidate) {
            return multiValueContains("scope", candidate) || multiValueContains("scp", candidate);
        }

        private boolean hasAttributeValue(String key, String candidate) {
            Set<String> multiValue = multiValueAttributes.get(key);
            if (multiValue != null) {
                return multiValue.contains(candidate);
            }
            return Objects.equals(attributes.getOrDefault(key, ""), candidate);
        }

        private boolean hasAnyAttributeValue(String key, Collection<String> candidates) {
            for (String candidate : candidates) {
                if (hasAttributeValue(key, candidate)) {
                    return true;
                }
            }
            return false;
        }

        private static Map<String, Set<String>> copyMultiValueAttributes(Map<String, Set<String>> source) {
            LinkedHashMap<String, Set<String>> copy = new LinkedHashMap<>();
            for (Map.Entry<String, Set<String>> entry : source.entrySet()) {
                copy.put(entry.getKey(), Collections.unmodifiableSet(new LinkedHashSet<>(entry.getValue())));
            }
            return copy;
        }

        public static final class Builder {
            private String name = "";
            private String externalPrincipal;
            private Set<String> externalGroups = new LinkedHashSet<>();
            private Map<String, String> attributes = new LinkedHashMap<>();
            private Map<String, Set<String>> multiValueAttributes = new LinkedHashMap<>();
            private boolean servicePrincipal;

            private Builder() {
            }

            public Builder name(String name) {
                this.name = name != null ? name : "";
                return this;
            }

            public Builder externalPrincipal(String externalPrincipal) {
                this.externalPrincipal = externalPrincipal;
                return this;
            }

            public Builder externalGroups(Set<String> externalGroups) {
                this.externalGroups = externalGroups != null ? externalGroups : new LinkedHashSet<>();
                return this;
            }

            public Builder attributes(Map<String, String> attributes) {
                this.attributes = attributes != null ? attributes : new LinkedHashMap<>();
                return this;
            }

            public Builder multiValueAttributes(Map<String, Set<String>> multiValueAttributes) {
                this.multiValueAttributes = multiValueAttributes != null ? multiValueAttributes : new LinkedHashMap<>();
                return this;
            }

            public Builder servicePrincipal(boolean servicePrincipal) {
                this.servicePrincipal = servicePrincipal;
                return this;
            }

            public EvaluationContext build() {
                return new EvaluationContext(this);
            }
        }
    }
}
