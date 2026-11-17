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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Consistency check of the physical plan visitor dispatch.
 *
 * <p>Every plan node that implements {@code accept()} itself hard codes the {@code visitXxx} method of
 * {@link PlanVisitor} that handles it. When such a node has a dedicated hook in {@link PlanVisitor},
 * the dispatch has to reach that hook: dispatching to a parent type silently turns the dedicated hook
 * into unreachable dead code, and nothing catches it - the compiler is happy and no runtime test fails.
 *
 * <p>The check reads the invoked method out of the compiled {@code accept()} body, so it covers every
 * physical plan node instead of a hand picked one. Nodes that inherit {@code accept()} from a parent
 * plan are out of scope by construction.
 */
public class PhysicalPlanVisitorDispatchTest {

    /** Guards against the class path scan silently finding nothing and making this test vacuous. */
    private static final int MIN_CHECKED_PLAN_CLASSES = 30;

    private static final String PLAN_PACKAGE = "org.apache.doris.nereids.trees.plans.physical";

    private static final String ACCEPT_DESCRIPTOR =
            "(Lorg/apache/doris/nereids/trees/plans/visitor/PlanVisitor;Ljava/lang/Object;)Ljava/lang/Object;";

    @Test
    public void acceptDispatchesToItsOwnMostSpecificVisitorMethod() throws Exception {
        Path classesRoot = classesRoot();
        Path packageRoot = classesRoot.resolve(PLAN_PACKAGE.replace('.', '/'));
        List<String> violations = new ArrayList<>();
        int checked = 0;

        for (Path classFile : planClassFiles(packageRoot)) {
            Class<?> planClass = Class.forName(toClassName(packageRoot, classFile), false,
                    PhysicalPlanVisitorDispatchTest.class.getClassLoader());
            if (!Plan.class.isAssignableFrom(planClass) || Modifier.isAbstract(planClass.getModifiers())
                    || !declaresAccept(planClass)) {
                continue;
            }
            String dispatched = dispatchedVisitorMethod(classFile);
            if (dispatched == null) {
                // accept() does not dispatch through a visit method, nothing to verify.
                continue;
            }
            checked++;
            String expected = mostSpecificVisitorMethod(planClass);
            if (!dispatched.equals(expected)) {
                violations.add(planClass.getSimpleName() + ".accept() dispatches to " + dispatched
                        + "(), but the most specific visitor method is " + expected + "()");
            }
        }

        Assertions.assertTrue(violations.isEmpty(),
                "accept() dispatches to the wrong visitor method:\n" + String.join("\n", violations));
        Assertions.assertTrue(checked >= MIN_CHECKED_PLAN_CLASSES,
                "only " + checked + " physical plan classes were checked, the class path scan is broken");
    }

    private static Path classesRoot() throws Exception {
        URL location = PlanVisitor.class.getProtectionDomain().getCodeSource().getLocation();
        Path root = Paths.get(location.toURI());
        Assumptions.assumeTrue(Files.isDirectory(root),
                "plan classes are not available as a directory: " + root);
        return root;
    }

    private static List<Path> planClassFiles(Path packageRoot) throws IOException {
        Assumptions.assumeTrue(Files.isDirectory(packageRoot), "no plans directory: " + packageRoot);
        try (Stream<Path> files = Files.walk(packageRoot)) {
            return files.filter(path -> path.toString().endsWith(".class"))
                    .filter(path -> !path.toString().contains("$"))
                    .filter(path -> !path.getFileName().toString().equals("package-info.class"))
                    .collect(Collectors.toList());
        }
    }

    private static String toClassName(Path packageRoot, Path classFile) {
        String relative = packageRoot.relativize(classFile).toString();
        return PLAN_PACKAGE + "."
                + relative.substring(0, relative.length() - ".class".length()).replace('/', '.');
    }

    private static boolean declaresAccept(Class<?> planClass) {
        try {
            planClass.getDeclaredMethod("accept", PlanVisitor.class, Object.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    /** Reads the visitor method invoked inside {@code accept()} from the compiled class file. */
    private static String dispatchedVisitorMethod(Path classFile) throws IOException {
        Set<String> invoked = new LinkedHashSet<>();
        byte[] bytes;
        try (InputStream in = Files.newInputStream(classFile)) {
            bytes = in.readAllBytes();
        }
        new ClassReader(bytes).accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor,
                    String signature, String[] exceptions) {
                if (!"accept".equals(name) || !ACCEPT_DESCRIPTOR.equals(descriptor)) {
                    return null;
                }
                return new MethodVisitor(Opcodes.ASM9) {
                    @Override
                    public void visitMethodInsn(int opcode, String owner, String methodName,
                            String methodDescriptor, boolean isInterface) {
                        if (methodName.startsWith("visit")) {
                            invoked.add(methodName);
                        }
                    }
                };
            }
        }, ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
        return invoked.size() == 1 ? invoked.iterator().next() : null;
    }

    /**
     * The visit method whose visited parameter is the closest super type of {@code planClass}, i.e. the
     * one declared for this exact plan node when such a hook exists.
     */
    private static String mostSpecificVisitorMethod(Class<?> planClass) {
        Method mostSpecific = null;
        for (Method method : PlanVisitor.class.getMethods()) {
            if (!method.getName().startsWith("visit") || method.getParameterCount() != 2) {
                continue;
            }
            Class<?> visited = method.getParameterTypes()[0];
            if (visited.isAssignableFrom(planClass)
                    && (mostSpecific == null
                            || mostSpecific.getParameterTypes()[0].isAssignableFrom(visited))) {
                mostSpecific = method;
            }
        }
        return mostSpecific == null ? null : mostSpecific.getName();
    }
}
