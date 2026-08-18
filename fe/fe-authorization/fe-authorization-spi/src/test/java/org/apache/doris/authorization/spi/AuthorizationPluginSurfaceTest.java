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

package org.apache.doris.authorization.spi;

import org.apache.doris.authorization.AccessRequirements;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Freezes the AUTHORIZATION plugin API surface, so that changing it cannot happen without also deciding the
 * version consequence.
 *
 * <p><b>Why this exists.</b> A plugin loaded from {@code plugins/authorization/} was compiled against some
 * release's version of these types and is admitted by matching majors alone
 * ({@code Doris-Authorization-Plugin-Api-Version}). That is only sound if "major" really means <em>any</em>
 * change to what a plugin can see - additions included. No unit test can prove somebody bumped the property
 * (a test sees the current state, never the delta), so this is a speed bump rather than a gate: it makes the
 * change visible in review, in the same commit, with the reason spelled out in the failure message.
 *
 * <p><b>Regenerating.</b> Run this test, copy the "actual" block out of the failure message into
 * {@code src/test/resources/authorization-plugin-surface.txt}, and bump the major of
 * {@code authorization.plugin.api.version} in {@code fe/fe-authorization/pom.xml} in the SAME commit.
 *
 * <h2>Why the surface is computed rather than listed</h2>
 *
 * <p>The other families freeze a hand-written list of SPI interfaces. That is enough for them because what
 * crosses their boundary is data a plugin passes through. It is not enough here: an authorization plugin
 * <em>decides</em> with the fe-authorization-api vocabulary, and one more {@code ResourceKind} or
 * {@code AccessAction} constant silently turns every deployed plugin's "a kind I do not recognise" branch -
 * which the contract requires to be a refusal - into a denial of something that used to be allowed. Nobody
 * rebuilt those plugins, and a hand-written list would have stayed green.
 *
 * <p>So the frozen set is a closure: start from what a plugin implements or is handed, and follow every
 * signature, field, nested type and supertype that stays inside {@code org.apache.doris}. A type the closure
 * cannot reach is a type no plugin can encounter, so leaving it out is not a gap.
 *
 * <p>{@code Plugin} / {@code PluginFactory} / {@code PluginContext} from fe-extension-spi are frozen here
 * too, and identically in the other families' baselines. They are loaded parent-first for every family, so a
 * change to them breaks all plugin kinds at once - and turns all baselines red at once, each asking for its
 * own bump.
 */
public class AuthorizationPluginSurfaceTest {

    private static final String BASELINE_RESOURCE = "/authorization-plugin-surface.txt";

    /**
     * Where the closure starts: the two interfaces a plugin implements, the context the engine hands it, the
     * three shared plugin types - and the named requirements, which no signature mentions because a plugin
     * reads them as constants to recognise which question it is being asked.
     */
    private static final List<Class<?>> SEED_TYPES = Arrays.asList(
            AuthorizationPluginFactory.class,
            AuthorizationPlugin.class,
            AuthorizationContext.class,
            AccessRequirements.class,
            org.apache.doris.extension.spi.Plugin.class,
            org.apache.doris.extension.spi.PluginFactory.class,
            org.apache.doris.extension.spi.PluginContext.class);

    @Test
    public void pluginApiSurfaceMatchesRecordedBaseline() throws IOException {
        TreeSet<String> actual = renderSurface();
        TreeSet<String> expected = readBaseline();

        TreeSet<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        TreeSet<String> added = new TreeSet<>(actual);
        added.removeAll(expected);

        Assertions.assertTrue(missing.isEmpty() && added.isEmpty(),
                "The AUTHORIZATION plugin API surface changed.\n"
                        + "  gone from the baseline (removed, renamed, or re-signed): " + missing + "\n"
                        + "  new since the baseline: " + added + "\n"
                        + "THIS IS A MAJOR CHANGE - the same commit that refreshes src/test/resources"
                        + BASELINE_RESOURCE + " must increment the major of"
                        + " <authorization.plugin.api.version> in fe/fe-authorization/pom.xml (and zero its"
                        + " minor).\n"
                        + "Full actual surface:\n" + String.join("\n", actual));
    }

    /**
     * The major of the served API version tracks the surface recorded above.
     *
     * <p>Written as a literal on purpose. Derived from the pom it would agree with itself whatever the pom
     * said, and what has to be caught is exactly the commit that refreshes the baseline without touching the
     * version: a plugin built against the old major would then be admitted and run against a contract it never
     * compiled against. The sibling gate in {@code ConnectorPluginSurfaceTest} is written the same way.
     */
    @Test
    public void authorizationApiMajorTracksTheRecordedSurfaceChange() throws IOException {
        Properties version = new Properties();
        try (InputStream in = AuthorizationPlugin.class.getResourceAsStream(
                "/META-INF/doris/authorization-plugin-api-version.properties")) {
            Assertions.assertNotNull(in, "missing authorization plugin API version resource");
            version.load(in);
        }
        // First published surface of this family.
        Assertions.assertEquals("1.0", version.getProperty("api.version"));
    }

    @Test
    public void theClosureReachesTheDecisionVocabulary() {
        // Guards the mechanism above rather than the surface itself: were the walk to stop at the SPI
        // interfaces, the baseline would still match on the day someone adds a resource kind, and the
        // check this class exists for would be silently gone.
        TreeSet<String> surface = renderSurface();
        for (String required : new String[] {
                "org.apache.doris.authorization.ResourceKind@TABLE",
                "org.apache.doris.authorization.AccessAction@SELECT",
                "org.apache.doris.authorization.AuthorizedResource$Table!final class",
                "org.apache.doris.authorization.RowFilterMergeType@PERMISSIVE"}) {
            Assertions.assertTrue(surface.contains(required),
                    "the frozen surface no longer reaches " + required
                            + "; a plugin decides with that type, so it must be frozen. Surface:\n"
                            + String.join("\n", surface));
        }
    }

    /**
     * One line per element a plugin can see, keyed by the type it is reachable on rather than by whichever
     * supertype declares it: what matters is what a plugin can call on the type it was handed, so moving a
     * default method up or down a super-interface chain is not by itself a surface change.
     */
    private static TreeSet<String> renderSurface() {
        TreeSet<String> rendered = new TreeSet<>();
        Set<Class<?>> visited = new HashSet<>();
        Deque<Class<?>> pending = new ArrayDeque<>(SEED_TYPES);
        while (!pending.isEmpty()) {
            Class<?> type = pending.poll();
            if (!isFrozen(type) || !visited.add(type)) {
                continue;
            }
            render(type, rendered, pending);
        }
        return rendered;
    }

    /** In scope iff it is one of ours: JDK types are a dependency of the surface, not part of it. */
    private static boolean isFrozen(Class<?> type) {
        Class<?> element = type;
        while (element.isArray()) {
            element = element.getComponentType();
        }
        return !element.isPrimitive() && isOurs(element);
    }

    /**
     * Whether we are the ones who declared it. Members inherited from the JDK are excluded for a practical
     * reason as much as a conceptual one: {@code Enum} and {@code Throwable} grow methods between Java
     * releases ({@code describeConstable} arrived in Java 12), and a baseline that recorded them would go red
     * on a compiler upgrade and demand a major bump nothing about this SPI justifies - training whoever hits
     * it to regenerate without reading.
     */
    private static boolean isOurs(Class<?> declaringClass) {
        return declaringClass.getName().startsWith("org.apache.doris.");
    }

    private static void render(Class<?> type, TreeSet<String> rendered, Deque<Class<?>> pending) {
        rendered.add(type.getName() + "!" + kindOf(type));
        if (type.isEnum()) {
            for (Object constant : type.getEnumConstants()) {
                rendered.add(type.getName() + "@" + ((Enum<?>) constant).name());
            }
        }
        for (Class<?> nested : type.getClasses()) {
            // A closed hierarchy is expressed as nested final subclasses, which no signature mentions
            // although every plugin casts to them.
            pending.add(nested);
        }
        pending.add(type.getSuperclass() == null ? Object.class : type.getSuperclass());
        pending.addAll(Arrays.asList(type.getInterfaces()));

        for (Field field : type.getFields()) {
            if (field.isSynthetic() || !isOurs(field.getDeclaringClass()) || field.isEnumConstant()) {
                // Enum constants are recorded above, in the form that says what actually matters about
                // them - that the constant exists - rather than restating their type.
                continue;
            }
            rendered.add(type.getName() + "$" + field.getName() + ":" + field.getGenericType().getTypeName()
                    + modifiersOf(field.getModifiers(), false));
            expand(field.getGenericType(), pending);
        }
        for (Constructor<?> constructor : type.getConstructors()) {
            if (constructor.isSynthetic()) {
                continue;
            }
            rendered.add(signature(type, "<init>", constructor.getGenericParameterTypes(), void.class));
            expandAll(constructor.getGenericParameterTypes(), pending);
        }
        for (Method method : type.getMethods()) {
            if (method.isSynthetic() || !isOurs(method.getDeclaringClass())
                    || (type.isEnum() && Modifier.isStatic(method.getModifiers())
                            && ("values".equals(method.getName()) || "valueOf".equals(method.getName())))) {
                continue;
            }
            rendered.add(signature(type, method.getName(), method.getGenericParameterTypes(),
                    method.getGenericReturnType())
                    + modifiersOf(method.getModifiers(), method.getDeclaringClass().isInterface()));
            expandAll(method.getGenericParameterTypes(), pending);
            expand(method.getGenericReturnType(), pending);
            expandAll(method.getGenericExceptionTypes(), pending);
        }
    }

    private static void expandAll(Type[] types, Deque<Class<?>> pending) {
        for (Type type : types) {
            expand(type, pending);
        }
    }

    /**
     * Follows a declared type to every class mentioned in it, type arguments included. Erasure is not good
     * enough here: {@code List<RowFilterSpec>} erases to {@code java.util.List}, and the payload types a
     * plugin builds its answers out of are reachable only through the argument.
     *
     * <p>Iterative with a seen set rather than recursive, because a type variable can name itself:
     * {@code Enum<E extends Enum<E>>} arrives on every enum's {@code compareTo} and walks forever.
     */
    private static void expand(Type root, Deque<Class<?>> pending) {
        Set<Type> seen = new HashSet<>();
        Deque<Type> todo = new ArrayDeque<>();
        todo.add(root);
        while (!todo.isEmpty()) {
            Type type = todo.poll();
            if (type == null || !seen.add(type)) {
                continue;
            }
            if (type instanceof Class) {
                Class<?> clazz = (Class<?>) type;
                pending.add(clazz.isArray() ? clazz.getComponentType() : clazz);
            } else if (type instanceof ParameterizedType) {
                ParameterizedType parameterized = (ParameterizedType) type;
                todo.add(parameterized.getRawType());
                todo.addAll(Arrays.asList(parameterized.getActualTypeArguments()));
            } else if (type instanceof GenericArrayType) {
                todo.add(((GenericArrayType) type).getGenericComponentType());
            } else if (type instanceof WildcardType) {
                todo.addAll(Arrays.asList(((WildcardType) type).getUpperBounds()));
                todo.addAll(Arrays.asList(((WildcardType) type).getLowerBounds()));
            } else if (type instanceof TypeVariable) {
                todo.addAll(Arrays.asList(((TypeVariable<?>) type).getBounds()));
            }
        }
    }

    /**
     * The declaration kind, because it is part of what a plugin may do with a type: an interface that becomes
     * a class stops being implementable, and a final class that stops being final stops being a closed set
     * the engine can enumerate.
     */
    private static String kindOf(Class<?> type) {
        if (type.isEnum()) {
            return "enum";
        }
        if (type.isInterface()) {
            return "interface";
        }
        if (Modifier.isAbstract(type.getModifiers())) {
            return "abstract class";
        }
        return Modifier.isFinal(type.getModifiers()) ? "final class" : "class";
    }

    /**
     * The modifiers of one member that a plugin's compiled code depends on, as {@code " [a, b]"} or nothing.
     *
     * <p>Recorded because the signature alone does not say whether a plugin has to implement a method. Every
     * "silence means refusal" default in this contract - {@code getRowFilters}, {@code getDataMasks},
     * {@code checkPrivilege} - is a {@code default} method, and turning one into {@code abstract} renders a
     * byte-identical line: the case stays green, the mandatory major bump is not triggered, and every deployed
     * plugin relying on that default throws {@code AbstractMethodError} on the first governed {@code SELECT}.
     * {@code static} and {@code final} are here for the same reason from the other direction - a plugin cannot
     * override what is final, and cannot inherit what is static.
     */
    private static String modifiersOf(int modifiers, boolean declaredInInterface) {
        List<String> interesting = new ArrayList<>();
        if (Modifier.isStatic(modifiers)) {
            interesting.add("static");
        }
        if (Modifier.isAbstract(modifiers)) {
            interesting.add("abstract");
        } else if (declaredInInterface && !Modifier.isStatic(modifiers)) {
            // The distinction this exists for: an interface method that is not abstract carries a body every
            // plugin may inherit instead of writing one.
            interesting.add("default");
        }
        if (Modifier.isFinal(modifiers)) {
            interesting.add("final");
        }
        return interesting.isEmpty() ? "" : " [" + String.join(", ", interesting) + "]";
    }

    private static String signature(Class<?> owner, String name, Type[] params, Type returnType) {
        StringBuilder sb = new StringBuilder(owner.getName()).append('#').append(name).append('(');
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(params[i].getTypeName());
        }
        return sb.append("):").append(returnType.getTypeName()).toString();
    }

    private static TreeSet<String> readBaseline() throws IOException {
        TreeSet<String> baseline = new TreeSet<>();
        try (InputStream in = AuthorizationPluginSurfaceTest.class.getResourceAsStream(BASELINE_RESOURCE)) {
            Assertions.assertNotNull(in, "missing test resource " + BASELINE_RESOURCE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    baseline.add(line.trim());
                }
            }
        }
        return baseline;
    }
}
