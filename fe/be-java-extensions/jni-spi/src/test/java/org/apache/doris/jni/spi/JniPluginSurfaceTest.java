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

package org.apache.doris.jni.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Freezes the JNI plugin API surface, so that changing it cannot happen without also deciding the
 * version consequence.
 *
 * <p><b>Why this exists.</b> {@code <jni.plugin.api.version>} in {@code fe/be-java-extensions/pom.xml}
 * decides which BE a given plugin directory may load into: it is stamped into every plugin jar's
 * {@code Doris-Jni-Plugin-Api-Version} manifest attribute and compared, major against major, by
 * {@code PluginRuntime}. The rule attached to it is blunt - <em>any</em> change to the surface below,
 * adding a type or a method just as much as removing or re-signing one, is a MAJOR change - and
 * nothing enforces it: every plugin in this repo is rebuilt from the same tree, so a surface change
 * with no bump compiles, tests green, and only breaks a plugin built somewhere else.
 *
 * <p>{@link JniScannerContractTest} covers a narrower question - the ten signatures BE resolves by
 * name on {@code JniScanner} - and it exists because those break loudly at scan time. This one is
 * about the whole shared layer, including the parts BE never touches but plugins compile against.
 *
 * <p><b>Regenerating.</b> Run this test, copy the "actual" block out of the failure message into
 * {@code src/test/resources/jni-plugin-surface.txt}, and bump the major of
 * {@code jni.plugin.api.version} in {@code fe/be-java-extensions/pom.xml} in the SAME commit.
 *
 * <p>Signatures are recorded with their return type: a changed return type is a MAJOR change by the
 * same definition, and a name-and-parameters-only record cannot see it. Fields, constructors,
 * nested types and the modifiers that matter are recorded for the same reason - see
 * {@link #renderSurface}.
 */
public class JniPluginSurfaceTest {

    private static final String BASELINE_RESOURCE = "/jni-plugin-surface.txt";

    /**
     * Pins the literal so that the version and the baseline below move together. Regenerating the
     * baseline without touching this line, or the other way round, is the mistake this catches.
     */
    @Test
    public void jniPluginApiMajorTracksTheRecordedSurfaceChange() {
        // getTableSchema/parseTableSchema removed (never implemented by any plugin and never
        // called by BE), and JniWriter no longer parses required_fields/columns_types into two
        // fields no writer read.
        Assertions.assertEquals("3.0", SpiVersion.version());
    }

    /** The package the frozen surface is, entirely: everything shipped in doris-jni-spi.jar. */
    private static final String SPI_PACKAGE = "org/apache/doris/jni/spi/";

    /**
     * Anchors, so that a discovery that silently finds nothing cannot pass. Not the frozen set -
     * that is {@link #frozenTypes()}, derived from the artifact - just the handful whose absence
     * means the walk broke rather than that the SPI shrank.
     */
    private static final List<String> SURFACE_ANCHORS = Arrays.asList(
            "org.apache.doris.jni.spi.DorisPlugin",
            "org.apache.doris.jni.spi.JniScanner",
            "org.apache.doris.jni.spi.JniWriter",
            "org.apache.doris.jni.spi.vec.VectorTable");

    @Test
    public void pluginApiSurfaceMatchesRecordedBaseline() throws IOException {
        TreeSet<String> actual = renderSurface();
        TreeSet<String> expected = readBaseline();

        TreeSet<String> missing = new TreeSet<>(expected);
        missing.removeAll(actual);
        TreeSet<String> added = new TreeSet<>(actual);
        added.removeAll(expected);

        Assertions.assertTrue(missing.isEmpty() && added.isEmpty(),
                "The JNI plugin API surface changed.\n"
                        + "  gone from the baseline (removed, renamed, or re-signed): " + missing + "\n"
                        + "  new since the baseline: " + added + "\n"
                        + "THIS IS A MAJOR CHANGE - the same commit that refreshes src/test/resources"
                        + BASELINE_RESOURCE + " must increment the major of <jni.plugin.api.version>"
                        + " in fe/be-java-extensions/pom.xml (and zero its minor), and the literal in"
                        + " jniPluginApiMajorTracksTheRecordedSurfaceChange above.\n"
                        + "Full actual surface:\n" + String.join("\n", actual));
    }

    /**
     * One line per member reachable on a frozen type, keyed by that type rather than by the class
     * that happens to declare it: what matters is what a plugin can call on the type it was handed.
     * Protected members are included as well - a plugin subclasses {@code JniScanner} and
     * {@code JniWriter}, so their protected hooks are as much of its contract as the public ones.
     *
     * <p>Methods, FIELDS and CONSTRUCTORS, each carrying the modifiers that decide whether a plugin
     * can still do what it did. Methods alone were not enough, and the gap was not hypothetical:
     * the commit that introduced this baseline removed two protected fields from {@code JniWriter}
     * in the same breath, a surface change this file could not express and therefore did not
     * require a major bump for. What the modifiers buy: {@code final} is load-bearing on the JNI
     * entry points (BE resolves them on the base class and a subclass override would never run),
     * {@code abstract} is the difference between a hook a plugin must implement and one it may,
     * and {@code static} changes how a member is reached at all. Visibility is recorded for the
     * same reason - public to protected is a break for every caller outside the hierarchy.
     *
     * <p>Nested types are frozen too, reached from their enclosing type: {@code ColumnType$Type}
     * and {@code NativeColumnValue$NativeValue} are as much a part of what a plugin compiles
     * against as the types that hand them out. Enum constants arrive as fields, which is what they
     * are.
     *
     * <p>Four further things are recorded because each of them is a MAJOR break this file used to
     * be blind to, and each has a live instance in this SPI:
     *
     * <ul>
     *   <li><b>Generic types</b>, not erasures. {@code getStatistics()} erases to
     *       {@code java.util.Map}, so changing its type arguments was silent.</li>
     *   <li><b>{@code throws} clauses.</b> {@code JniScanner.openInternal()} declares
     *       {@code IOException}; narrowing it breaks every subclass that catches it, and widening
     *       it breaks every caller.</li>
     *   <li><b>The frozen type's own declaration</b> - modifiers, superclass and interfaces.
     *       {@code ThreadContextClassLoader implements Closeable} is what lets a plugin use it in
     *       try-with-resources, and dropping the interface changed no member line.</li>
     *   <li><b>The VALUE of a compile-time constant.</b> {@code SpiVersion.MANIFEST_ATTRIBUTE} is
     *       inlined into plugin bytecode at compile time, so changing the string breaks plugins
     *       that were ALREADY BUILT while every line here stays identical. Only true constants
     *       (JLS constant variables, which carry a ConstantValue attribute) are rendered - a
     *       {@code static final} assigned in a static initializer, such as
     *       {@code OffHeap.BYTE_ARRAY_OFFSET}, holds a JVM-dependent value that must not become
     *       part of a baseline, and is not inlined into callers either.</li>
     * </ul>
     */
    private static TreeSet<String> renderSurface() throws IOException {
        TreeSet<String> rendered = new TreeSet<>();
        for (Class<?> frozen : frozenClosure()) {
            rendered.add(declaration(frozen));
            for (Constructor<?> c : frozen.getDeclaredConstructors()) {
                if (c.isSynthetic() || Modifier.isPrivate(c.getModifiers())) {
                    continue;
                }
                rendered.add(new StringBuilder(frozen.getName()).append("#<init>")
                        .append(parameters(c)).append(thrown(c))
                        .append(modifiers(c.getModifiers())).toString());
            }
            // Constructors are not inherited; everything else is, and is recorded against the type
            // a plugin actually holds rather than the level that declares it.
            //
            // Stops at Enum as well as Object: a frozen enum (ColumnType$Type) would otherwise
            // drag in java.lang.Enum's own members - name(), ordinal(), compareTo(),
            // describeConstable() and the rest - which are the JDK's surface and not this SPI's.
            // Recording them made the baseline JDK-dependent, and the prescribed response to a
            // failure here is a major bump, which rejects every deployed plugin. A false alarm
            // whose remedy is an outage is worse than no alarm.
            for (Class<?> level = frozen;
                    level != null && level != Object.class && level != Enum.class;
                    level = level.getSuperclass()) {
                Set<String> constants = constantValueFields(level);
                for (Method m : level.getDeclaredMethods()) {
                    if (m.isSynthetic() || Modifier.isPrivate(m.getModifiers())) {
                        continue;
                    }
                    rendered.add(new StringBuilder(frozen.getName()).append('#').append(m.getName())
                            .append(parameters(m)).append(':')
                            .append(m.getGenericReturnType().getTypeName())
                            .append(thrown(m)).append(modifiers(m.getModifiers())).toString());
                }
                for (Field f : level.getDeclaredFields()) {
                    if (f.isSynthetic() || Modifier.isPrivate(f.getModifiers())) {
                        continue;
                    }
                    StringBuilder line = new StringBuilder(frozen.getName()).append('#')
                            .append(f.getName()).append(':')
                            .append(f.getGenericType().getTypeName());
                    if (constants.contains(f.getName())) {
                        line.append('=').append(constantValue(f));
                    }
                    rendered.add(line.append(modifiers(f.getModifiers())).toString());
                }
            }
        }
        return rendered;
    }

    /** The type's own declaration: what it is, what it extends, what it implements. */
    private static String declaration(Class<?> frozen) {
        StringBuilder line = new StringBuilder(frozen.getName()).append("#<type>");
        if (frozen.isInterface()) {
            line.append(" interface");
        } else if (frozen.isEnum()) {
            // The superclass of an enum is java.lang.Enum<itself>, which says nothing beyond this.
            line.append(" enum");
        } else {
            line.append(" class extends ").append(frozen.getGenericSuperclass().getTypeName());
        }
        List<String> interfaces = new ArrayList<>();
        for (java.lang.reflect.Type i : frozen.getGenericInterfaces()) {
            interfaces.add(i.getTypeName());
        }
        java.util.Collections.sort(interfaces);
        if (!interfaces.isEmpty()) {
            line.append(" implements ").append(String.join(",", interfaces));
        }
        return line.append(modifiers(frozen.getModifiers())).toString();
    }

    /** The declared checked exceptions, sorted so the rendering does not depend on source order. */
    private static String thrown(Executable executable) {
        java.lang.reflect.Type[] exceptions = executable.getGenericExceptionTypes();
        if (exceptions.length == 0) {
            return "";
        }
        List<String> names = new ArrayList<>();
        for (java.lang.reflect.Type e : exceptions) {
            names.add(e.getTypeName());
        }
        java.util.Collections.sort(names);
        return " throws " + String.join(",", names);
    }

    /**
     * Names of the fields of {@code type} that are JLS constant variables, read out of the class
     * file rather than guessed from the modifiers.
     *
     * <p>Reflection cannot answer this: {@code static final int} looks the same whether the
     * compiler inlined it or a static initializer assigns it, and the difference is exactly what
     * matters - only the inlined ones are baked into an already-built plugin, and only they hold a
     * value that is the same on every JVM. The ConstantValue attribute is the compiler's own
     * record of which is which.
     *
     * <p>Reads only the constant pool and the field table, and never initializes the class.
     */
    private static Set<String> constantValueFields(Class<?> type) {
        String resource = type.getName();
        resource = "/" + resource.replace('.', '/') + ".class";
        try (InputStream in = JniPluginSurfaceTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                return java.util.Collections.emptySet();
            }
            return ClassFile.constantFields(new java.io.DataInputStream(new java.io.BufferedInputStream(in)));
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("cannot read the class file of " + type.getName()
                    + "; the surface baseline would silently lose its constant values", e);
        }
    }

    /** A constant's value, rendered so that a newline or a quote cannot break the one-line form. */
    private static String constantValue(Field field) {
        Object value;
        try {
            field.setAccessible(true);
            value = field.get(null);
        } catch (IllegalAccessException | RuntimeException e) {
            throw new IllegalStateException("cannot read the constant " + field, e);
        }
        if (!(value instanceof String)) {
            return String.valueOf(value);
        }
        StringBuilder quoted = new StringBuilder("\"");
        for (char c : ((String) value).toCharArray()) {
            if (c == '"' || c == '\\') {
                quoted.append('\\').append(c);
            } else if (c < 0x20 || c > 0x7e) {
                quoted.append(String.format("\\u%04x", (int) c));
            } else {
                quoted.append(c);
            }
        }
        return quoted.append('"').toString();
    }

    /** The frozen types plus every non-private type nested inside one of them, transitively. */
    /**
     * Every public top-level type shipped in the SPI package, read off the artifact this module
     * just built rather than listed by hand.
     *
     * <p>A hand-written list is the one thing this whole test cannot afford: adding a type to the
     * SPI is a MAJOR change by the contract above, and a list that has to be edited to notice one
     * is a gate that stays green through exactly the change it is for. Derived from the code source
     * of a class that is certainly in it, so the answer is "what is in the jar", not "what a test
     * remembered".
     *
     * <p>Nested types are not collected here - {@link #frozenClosure()} expands them from their
     * enclosing type, which also catches a nested type added to a class that was already frozen.
     */
    private static List<Class<?>> frozenTypes() throws IOException {
        List<String> binaryNames = new ArrayList<>();
        URL location = DorisPlugin.class.getProtectionDomain().getCodeSource().getLocation();
        Path root;
        try {
            root = Paths.get(location.toURI());
        } catch (URISyntaxException e) {
            throw new IOException("cannot read the SPI code source at " + location, e);
        }
        if (Files.isDirectory(root)) {
            Path packageRoot = root.resolve(SPI_PACKAGE);
            try (Stream<Path> entries = Files.walk(packageRoot)) {
                for (Path entry : (Iterable<Path>) entries::iterator) {
                    collectTopLevel(root.relativize(entry).toString().replace(File.separatorChar, '/'),
                            binaryNames);
                }
            }
        } else {
            try (ZipFile jar = new ZipFile(root.toFile())) {
                Enumeration<? extends ZipEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    collectTopLevel(entries.nextElement().getName(), binaryNames);
                }
            }
        }

        List<Class<?>> types = new ArrayList<>();
        for (String name : binaryNames) {
            try {
                Class<?> type = Class.forName(name, false, JniPluginSurfaceTest.class.getClassLoader());
                if (Modifier.isPublic(type.getModifiers())) {
                    types.add(type);
                }
            } catch (ClassNotFoundException e) {
                throw new IOException("the SPI artifact holds " + name + " but it cannot be loaded", e);
            }
        }
        types.sort(Comparator.comparing(Class::getName));
        List<String> discovered = new ArrayList<>();
        types.forEach(type -> discovered.add(type.getName()));
        Assertions.assertTrue(discovered.containsAll(SURFACE_ANCHORS),
                "the SPI surface walk found " + discovered + ", which is missing one of the types"
                        + " that must always be there (" + SURFACE_ANCHORS + "). The walk is broken,"
                        + " not the surface - fix it before reading anything into a passing run.");
        return types;
    }

    /** Records {@code path} as a frozen candidate when it is a top-level class of the SPI package. */
    private static void collectTopLevel(String path, List<String> binaryNames) {
        if (!path.startsWith(SPI_PACKAGE) || !path.endsWith(".class")) {
            return;
        }
        String name = path.substring(0, path.length() - ".class".length());
        // Nested types come from their enclosing one, and package-info carries no members.
        if (name.indexOf('$') >= 0 || name.endsWith("/package-info")) {
            return;
        }
        binaryNames.add(name.replace('/', '.'));
    }

    private static List<Class<?>> frozenClosure() throws IOException {
        Set<Class<?>> seen = new LinkedHashSet<>();
        Deque<Class<?>> pending = new ArrayDeque<>(frozenTypes());
        while (!pending.isEmpty()) {
            Class<?> type = pending.removeFirst();
            if (!seen.add(type)) {
                continue;
            }
            for (Class<?> nested : type.getDeclaredClasses()) {
                if (!nested.isSynthetic() && !Modifier.isPrivate(nested.getModifiers())) {
                    pending.addLast(nested);
                }
            }
        }
        return new ArrayList<>(seen);
    }

    /** Generic, not erased: a changed type argument re-signs the method for every plugin. */
    private static String parameters(Executable executable) {
        StringBuilder sb = new StringBuilder("(");
        java.lang.reflect.Type[] params = executable.getGenericParameterTypes();
        for (int i = 0; i < params.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(params[i].getTypeName());
        }
        return sb.append(')').toString();
    }

    /** Only the modifiers a plugin can be broken by; ordered so the rendering is stable. */
    private static String modifiers(int modifiers) {
        List<String> kept = new ArrayList<>();
        if (Modifier.isPublic(modifiers)) {
            kept.add("public");
        }
        if (Modifier.isProtected(modifiers)) {
            kept.add("protected");
        }
        if (Modifier.isStatic(modifiers)) {
            kept.add("static");
        }
        if (Modifier.isFinal(modifiers)) {
            kept.add("final");
        }
        if (Modifier.isAbstract(modifiers)) {
            kept.add("abstract");
        }
        return " [" + String.join(",", kept) + "]";
    }

    /**
     * Just enough of the class file format to answer "does this field carry a ConstantValue
     * attribute": the constant pool (for attribute names) and the field table.
     *
     * <p>JVMS 4.1. Nothing here interprets the value - {@link #constantValue} reads that
     * reflectively once this has said the field is a constant, at which point the value is
     * guaranteed to be the compiler's and therefore the same on every JVM.
     */
    private static final class ClassFile {

        private static final int CONSTANT_UTF8 = 1;
        private static final int CONSTANT_INTEGER = 3;
        private static final int CONSTANT_FLOAT = 4;
        private static final int CONSTANT_LONG = 5;
        private static final int CONSTANT_DOUBLE = 6;
        private static final int CONSTANT_CLASS = 7;
        private static final int CONSTANT_STRING = 8;
        private static final int CONSTANT_FIELDREF = 9;
        private static final int CONSTANT_METHODREF = 10;
        private static final int CONSTANT_INTERFACE_METHODREF = 11;
        private static final int CONSTANT_NAME_AND_TYPE = 12;
        private static final int CONSTANT_METHOD_HANDLE = 15;
        private static final int CONSTANT_METHOD_TYPE = 16;
        private static final int CONSTANT_DYNAMIC = 17;
        private static final int CONSTANT_INVOKE_DYNAMIC = 18;
        private static final int CONSTANT_MODULE = 19;
        private static final int CONSTANT_PACKAGE = 20;

        private ClassFile() {
        }

        static Set<String> constantFields(java.io.DataInputStream in) throws IOException {
            if (in.readInt() != 0xCAFEBABE) {
                throw new IOException("not a class file");
            }
            in.readUnsignedShort(); // minor
            in.readUnsignedShort(); // major
            String[] pool = readConstantPool(in);
            in.readUnsignedShort(); // access_flags
            in.readUnsignedShort(); // this_class
            in.readUnsignedShort(); // super_class
            skipBytes(in, 2 * in.readUnsignedShort()); // interfaces
            Set<String> constants = new LinkedHashSet<>();
            int fields = in.readUnsignedShort();
            for (int i = 0; i < fields; i++) {
                in.readUnsignedShort(); // access_flags
                String name = pool[in.readUnsignedShort()];
                in.readUnsignedShort(); // descriptor_index
                int attributes = in.readUnsignedShort();
                for (int a = 0; a < attributes; a++) {
                    String attribute = pool[in.readUnsignedShort()];
                    long length = in.readInt() & 0xFFFFFFFFL;
                    if ("ConstantValue".equals(attribute)) {
                        constants.add(name);
                    }
                    skipBytes(in, length);
                }
            }
            return constants;
        }

        /** UTF8 entries by index; everything else is a null slot nobody here asks for. */
        private static String[] readConstantPool(java.io.DataInputStream in) throws IOException {
            int count = in.readUnsignedShort();
            String[] pool = new String[count];
            for (int i = 1; i < count; i++) {
                int tag = in.readUnsignedByte();
                switch (tag) {
                    case CONSTANT_UTF8:
                        pool[i] = in.readUTF();
                        break;
                    case CONSTANT_INTEGER:
                    case CONSTANT_FLOAT:
                    case CONSTANT_FIELDREF:
                    case CONSTANT_METHODREF:
                    case CONSTANT_INTERFACE_METHODREF:
                    case CONSTANT_NAME_AND_TYPE:
                    case CONSTANT_DYNAMIC:
                    case CONSTANT_INVOKE_DYNAMIC:
                        skipBytes(in, 4);
                        break;
                    case CONSTANT_LONG:
                    case CONSTANT_DOUBLE:
                        skipBytes(in, 8);
                        // "8-byte constants take up two entries" - JVMS 4.4.5.
                        i++;
                        break;
                    case CONSTANT_CLASS:
                    case CONSTANT_STRING:
                    case CONSTANT_METHOD_TYPE:
                    case CONSTANT_MODULE:
                    case CONSTANT_PACKAGE:
                        skipBytes(in, 2);
                        break;
                    case CONSTANT_METHOD_HANDLE:
                        skipBytes(in, 3);
                        break;
                    default:
                        throw new IOException("unknown constant pool tag " + tag
                                + "; this JDK writes a class file format this reader predates");
                }
            }
            return pool;
        }

        private static void skipBytes(java.io.DataInputStream in, long count) throws IOException {
            long left = count;
            while (left > 0) {
                long skipped = in.skip(left);
                if (skipped <= 0) {
                    if (in.read() < 0) {
                        throw new java.io.EOFException("truncated class file");
                    }
                    skipped = 1;
                }
                left -= skipped;
            }
        }
    }

    private static TreeSet<String> readBaseline() throws IOException {
        TreeSet<String> baseline = new TreeSet<>();
        try (InputStream in = JniPluginSurfaceTest.class.getResourceAsStream(BASELINE_RESOURCE)) {
            Assertions.assertNotNull(in, "missing test resource " + BASELINE_RESOURCE);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                String entry = line.trim();
                if (!entry.isEmpty() && !entry.startsWith("#")) {
                    baseline.add(entry);
                }
            }
        }
        return baseline;
    }
}
