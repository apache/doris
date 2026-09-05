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

package org.apache.doris.common.jni.vec;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class NestedProjectionTest {

    /**
     * A source type tree the test owns outright. java-common must not depend on paimon or fluss, and a
     * hand-built tree also lets a case state the exact field order a real source would have.
     */
    private static final class Src {
        private final NestedProjection.Kind kind;
        private final List<String> names;
        private final List<Src> children;

        private Src(NestedProjection.Kind kind, List<String> names, List<Src> children) {
            this.kind = kind;
            this.names = names;
            this.children = children;
        }

        static Src scalar() {
            return new Src(NestedProjection.Kind.SCALAR, null, null);
        }

        static Src struct(String[] names, Src... children) {
            return new Src(NestedProjection.Kind.STRUCT, Arrays.asList(names), Arrays.asList(children));
        }

        static Src array(Src element) {
            return new Src(NestedProjection.Kind.ARRAY, null, Arrays.asList(element));
        }

        static Src map(Src key, Src value) {
            return new Src(NestedProjection.Kind.MAP, null, Arrays.asList(key, value));
        }
    }

    private static final NestedProjection.TypeSource<Src> SOURCE = new NestedProjection.TypeSource<Src>() {
        @Override
        public NestedProjection.Kind kindOf(Src type) {
            return type.kind;
        }

        @Override
        public List<String> structFieldNames(Src type) {
            return type.names;
        }

        @Override
        public Src structFieldType(Src type, int index) {
            return type.children.get(index);
        }

        @Override
        public Src arrayElementType(Src type) {
            return type.children.get(0);
        }

        @Override
        public Src mapKeyType(Src type) {
            return type.children.get(0);
        }

        @Override
        public Src mapValueType(Src type) {
            return type.children.get(1);
        }

        @Override
        public String describe(Src type) {
            return type.kind + (type.names == null ? "" : type.names.toString());
        }
    };

    private static NestedProjection<Src> project(String dorisType, Src source) {
        return NestedProjection.of(ColumnType.parseType("root", dorisType), source, SOURCE);
    }

    @Test
    public void structChildResolvesByNameNotByPosition() {
        // Doris asks for the THIRD source field first: a position-aligned implementation returns "city".
        Src source = Src.struct(new String[] {"city", "zip", "street"},
                Src.scalar(), Src.scalar(), Src.scalar());

        NestedProjection<Src> shape = project("struct<street:string,zip:int>", source);

        Assert.assertEquals(NestedProjection.Kind.STRUCT, shape.getKind());
        Assert.assertFalse(shape.isIdentity());
        Assert.assertEquals(2, shape.childCount());
        Assert.assertEquals(2, shape.sourceChildIndex(0));
        Assert.assertEquals(1, shape.sourceChildIndex(1));
    }

    @Test
    public void structChildNameMatchIsCaseInsensitive() {
        Src source = Src.struct(new String[] {"City"}, Src.scalar());

        Assert.assertEquals(0, project("struct<CITY:string>", source).sourceChildIndex(0));
    }

    @Test
    public void missingStructChildFailsLoudNamingTheField() {
        Src source = Src.struct(new String[] {"known"}, Src.scalar());

        try {
            project("struct<missing:int>", source);
            Assert.fail("a requested field that does not exist must not resolve silently");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("missing"));
        }
    }

    @Test
    public void shapeConflictFailsLoudShowingBothSides() {
        try {
            project("struct<a:int>", Src.array(Src.scalar()));
            Assert.fail("Doris STRUCT over a source ARRAY must not resolve");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("STRUCT"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("ARRAY"));
        }
    }

    @Test
    public void wholeStructRequestedInSourceOrderIsIdentity() {
        Src source = Src.struct(new String[] {"a", "b"}, Src.scalar(), Src.scalar());

        Assert.assertTrue(project("struct<a:int,b:int>", source).isIdentity());
    }

    @Test
    public void reorderedWholeStructIsNotIdentity() {
        // Same fields, different order: the consumer still has to remap, so identity must be false.
        Src source = Src.struct(new String[] {"a", "b"}, Src.scalar(), Src.scalar());

        Assert.assertFalse(project("struct<b:int,a:int>", source).isIdentity());
    }

    @Test
    public void arrayElementIsWalkedAndCarriesPruning() {
        Src source = Src.array(Src.struct(new String[] {"name", "score"}, Src.scalar(), Src.scalar()));

        NestedProjection<Src> shape = project("array<struct<score:int>>", source);

        Assert.assertEquals(NestedProjection.Kind.ARRAY, shape.getKind());
        Assert.assertFalse(shape.isIdentity());
        Assert.assertEquals(1, shape.elementProjection().sourceChildIndex(0));
    }

    @Test
    public void mapWalksTheKeyAndPrunesTheValue() {
        // The key is a STRUCT pruned to its second field ON PURPOSE: with a scalar key, "walked" and
        // "assumed identity" are indistinguishable, so a mutation that skips the key walk still passes.
        // (The engine never prunes a real map key; this only pins that the walker resolves it.)
        Src source = Src.map(
                Src.struct(new String[] {"k1", "k2"}, Src.scalar(), Src.scalar()),
                Src.struct(new String[] {"code", "label"}, Src.scalar(), Src.scalar()));

        NestedProjection<Src> shape = project("map<struct<k2:int>,struct<label:string>>", source);

        Assert.assertEquals(NestedProjection.Kind.MAP, shape.getKind());
        Assert.assertFalse(shape.keyProjection().isIdentity());
        Assert.assertEquals(1, shape.keyProjection().sourceChildIndex(0));
        Assert.assertEquals(1, shape.valueProjection().sourceChildIndex(0));
    }

    @Test
    public void ambiguousCaseInsensitiveMatchFailsLoudNamingBothFields() {
        // Exact match wins first; with no exact hit, TWO case-insensitive hits must not resolve to
        // whichever the source listed first -- that reads the wrong column with no error at all.
        Src source = Src.struct(new String[] {"ID", "Id"}, Src.scalar(), Src.scalar());

        try {
            project("struct<id:int>", source);
            Assert.fail("two case-insensitive matches must not resolve silently");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("ID"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("Id"));
        }
    }

    @Test
    public void exactMatchWinsOverCaseInsensitiveNeighbors() {
        // "City" and "city" both match "city" ignoring case, but an exact "city" exists: it must win
        // without tripping the ambiguity guard.
        Src source = Src.struct(new String[] {"City", "city"}, Src.scalar(), Src.scalar());

        Assert.assertEquals(1, project("struct<city:int>", source).sourceChildIndex(0));
    }

    @Test
    public void twoRequestedChildrenResolvingToOneSourceFieldFailsLoud() {
        // A channel that lowercases names (the legacy JNI type grammar does) can hand this class the
        // same requested name twice for a source with "a" and "A". Resolving both to source field 0
        // reads it twice and never reads the other -- wrong data with no error.
        Src source = Src.struct(new String[] {"a", "A"}, Src.scalar(), Src.scalar());

        try {
            project("struct<a:int,a:int>", source);
            Assert.fail("two requested children resolving to one source field must not resolve");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("a"));
        }
    }

    @Test
    public void scalarRequestOverComplexSourceFailsLoud() {
        // A Doris scalar over a source STRUCT is the same shape conflict as STRUCT-over-ARRAY with the
        // sides swapped; resolving it as scalar identity would hide a complex column behind Kind.SCALAR.
        try {
            project("int", Src.struct(new String[] {"a"}, Src.scalar()));
            Assert.fail("a Doris scalar over a source STRUCT must not resolve");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("SCALAR"));
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("STRUCT"));
        }
    }

    @Test
    public void scalarKeepsTheSourceTypeRatherThanTheDorisOne() {
        // D39: timestamp precision and CHAR length are not equivalent across the two type systems, and
        // fluss picks its accessor from the SOURCE type. The walker must hand the source type back.
        Src source = Src.scalar();

        NestedProjection<Src> shape = project("int", source);

        Assert.assertEquals(NestedProjection.Kind.SCALAR, shape.getKind());
        Assert.assertTrue(shape.isIdentity());
        Assert.assertSame(source, shape.getSourceType());
    }

    @Test
    public void nestingThreeLevelsDeepRemapsEveryLevel() {
        Src inner = Src.struct(new String[] {"x", "y"}, Src.scalar(), Src.scalar());
        Src outer = Src.struct(new String[] {"pad", "row"}, Src.scalar(), inner);
        Src source = Src.array(Src.map(Src.scalar(), outer));

        NestedProjection<Src> shape =
                project("array<map<string,struct<row:struct<y:string>>>>", source);

        NestedProjection<Src> value = shape.elementProjection().valueProjection();
        Assert.assertEquals(1, value.sourceChildIndex(0));
        Assert.assertEquals(1, value.child(0).sourceChildIndex(0));
    }

    @Test
    public void unsupportedColumnIsTreatedAsScalarIdentity() {
        // ColumnType marks a type Doris cannot represent as UNSUPPORTED. Walking into it would be a
        // guess; identity keeps the caller on its existing path.
        Src source = Src.scalar();

        Assert.assertTrue(NestedProjection.of(
                ColumnType.parseType("root", "unsupported"), source, SOURCE).isIdentity());
    }
}
