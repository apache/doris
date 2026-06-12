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

// Copyright (C) 2011 Google Inc.

package org.apache.doris.persist.gson;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.JsonReaderInternalAccess;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adapts values whose runtime type may differ from their declaration type. This
 * is necessary when a field's type is not the same type that GSON should create
 * when deserializing that field. For example, consider these types:
 *
 * <pre>
 * {
 *     &#64;code
 *     abstract class Shape {
 *         int x;
 *         int y;
 *     }
 *     class Circle extends Shape {
 *         int radius;
 *     }
 *     class Rectangle extends Shape {
 *         int width;
 *         int height;
 *     }
 *     class Diamond extends Shape {
 *         int width;
 *         int height;
 *     }
 *     class Drawing {
 *         Shape bottomShape;
 *         Shape topShape;
 *     }
 * }
 * </pre>
 * <p>
 * Without additional type information, the serialized JSON is ambiguous. Is
 * the bottom shape in this drawing a rectangle or a diamond?
 *
 * <pre>
 *    {@code
 *   {
 *     "bottomShape": {
 *       "width": 10,
 *       "height": 5,
 *       "x": 0,
 *       "y": 0
 *     },
 *     "topShape": {
 *       "radius": 2,
 *       "x": 4,
 *       "y": 1
 *     }
 *   }}
 * </pre>
 *
 * This class addresses this problem by adding type information to the
 * serialized JSON and honoring that type information when the JSON is
 * deserialized:
 *
 * <pre>
 *    {@code
 *   {
 *     "bottomShape": {
 *       "type": "Diamond",
 *       "width": 10,
 *       "height": 5,
 *       "x": 0,
 *       "y": 0
 *     },
 *     "topShape": {
 *       "type": "Circle",
 *       "radius": 2,
 *       "x": 4,
 *       "y": 1
 *     }
 *   }}
 * </pre>
 *
 * Both the type field name ({@code "type"}) and the type labels ({@code
 * "Rectangle"}) are configurable.
 *
 * <h3>Registering Types</h3>
 * Create a {@code RuntimeTypeAdapterFactory} by passing the base type and type
 * field
 * name to the {@link #of} factory method. If you don't supply an explicit type
 * field name, {@code "type"} will be used.
 *
 * <pre>
 * {
 *     &#64;code
 *     RuntimeTypeAdapterFactory<Shape> shapeAdapterFactory = RuntimeTypeAdapterFactory.of(Shape.class, "type");
 * }
 * </pre>
 *
 * Next register all of your subtypes. Every subtype must be explicitly
 * registered. This protects your application from injection attacks. If you
 * don't supply an explicit type label, the type's simple name will be used.
 *
 * <pre>
 *    {@code
 *   shapeAdapterFactory.registerSubtype(Rectangle.class, "Rectangle");
 *   shapeAdapterFactory.registerSubtype(Circle.class, "Circle");
 *   shapeAdapterFactory.registerSubtype(Diamond.class, "Diamond");
 * }
 * </pre>
 *
 * Finally, register the type adapter factory in your application's GSON
 * builder:
 *
 * <pre>
 * {
 *     &#64;code
 *     Gson gson = new GsonBuilder().registerTypeAdapterFactory(shapeAdapterFactory).create();
 * }
 * </pre>
 *
 * Like {@code GsonBuilder}, this API supports chaining:
 *
 * <pre>
 * {
 *     &#64;code
 *     RuntimeTypeAdapterFactory<Shape> shapeAdapterFactory = RuntimeTypeAdapterFactory.of(Shape.class)
 *             .registerSubtype(Rectangle.class).registerSubtype(Circle.class).registerSubtype(Diamond.class);
 * }
 * </pre>
 *
 * <h3>Serialization and deserialization</h3>
 * In order to serialize and deserialize a polymorphic object,
 * you must specify the base type explicitly.
 *
 * <pre>
 * {
 *     &#64;code
 *     Diamond diamond = new Diamond();
 *     String json = gson.toJson(diamond, Shape.class);
 * }
 * </pre>
 *
 * And then:
 *
 * <pre>
 * {
 *     &#64;code
 *     Shape shape = gson.fromJson(json, Shape.class);
 * }
 * </pre>
 */
public final class RuntimeTypeAdapterFactory<T> implements TypeAdapterFactory {
    private final Class<?> baseType;
    private final String typeFieldName;
    private final Map<String, Class<?>> labelToSubtype = new LinkedHashMap<String, Class<?>>();
    private final Map<Class<?>, String> subtypeToLabel = new LinkedHashMap<Class<?>, String>();

    private final boolean maintainType;
    private Class<? extends T> defaultType = null;
    // When enabled, write/read dispatch streams directly to/from the underlying
    // JsonWriter/JsonReader instead of materializing the whole object as a
    // JsonElement DOM tree. The produced bytes are identical to the tree mode
    // (the type field is still emitted as the first field). This matters for
    // huge objects like backup/restore jobs whose single journal entry can be
    // tens of MB: tree mode keeps a DOM dozens of times larger than the json
    // alive during write/read. Streaming read requires the type field to be the
    // first field, which holds for all Doris-generated journals/images since
    // tree mode always emits it first.
    private boolean streamingDispatch = false;

    private RuntimeTypeAdapterFactory(Class<?> baseType, String typeFieldName, boolean maintainType) {
        if (typeFieldName == null || baseType == null) {
            throw new NullPointerException();
        }
        this.baseType = baseType;
        this.typeFieldName = typeFieldName;
        this.maintainType = maintainType;
    }

    /**
     * Creates a new runtime type adapter using for {@code baseType} using {@code
     * typeFieldName} as the type field name. Type field names are case sensitive.
     * {@code maintainType} flag decide if the type will be stored in pojo or not.
     */
    public static <T> RuntimeTypeAdapterFactory<T> of(Class<T> baseType, String typeFieldName, boolean maintainType) {
        return new RuntimeTypeAdapterFactory<T>(baseType, typeFieldName, maintainType);
    }

    /**
     * Creates a new runtime type adapter using for {@code baseType} using {@code
     * typeFieldName} as the type field name. Type field names are case sensitive.
     */
    public static <T> RuntimeTypeAdapterFactory<T> of(Class<T> baseType, String typeFieldName) {
        return new RuntimeTypeAdapterFactory<T>(baseType, typeFieldName, false);
    }

    /**
     * Creates a new runtime type adapter for {@code baseType} using {@code "type"}
     * as
     * the type field name.
     */
    public static <T> RuntimeTypeAdapterFactory<T> of(Class<T> baseType) {
        return new RuntimeTypeAdapterFactory<T>(baseType, "type", false);
    }

    /**
     * Registers {@code type} identified by {@code label}. Labels are case
     * sensitive.
     *
     * @throws IllegalArgumentException
     *             if either {@code type} or {@code label}
     *             have already been registered on this type adapter.
     */
    public RuntimeTypeAdapterFactory<T> registerSubtype(Class<? extends T> type, String label) {
        if (type == null || label == null) {
            throw new NullPointerException();
        }
        if (subtypeToLabel.containsKey(type) || labelToSubtype.containsKey(label)) {
            throw new IllegalArgumentException("types and labels must be unique");
        }
        labelToSubtype.put(label, type);
        subtypeToLabel.put(type, label);
        return this;
    }

    /**
     * Registers {@code type} identified by its {@link Class#getSimpleName simple
     * name}. Labels are case sensitive.
     *
     * @throws IllegalArgumentException
     *             if either {@code type} or its simple name
     *             have already been registered on this type adapter.
     */
    public RuntimeTypeAdapterFactory<T> registerSubtype(Class<? extends T> type) {
        return registerSubtype(type, type.getSimpleName());
    }

    /**
     * Enables streaming dispatch. Only valid for factories without
     * {@code maintainType} and without a default subtype, where the type field
     * is guaranteed to be the first json field.
     */
    public RuntimeTypeAdapterFactory<T> withStreamingDispatch() {
        if (maintainType) {
            throw new IllegalStateException("streaming dispatch does not support maintainType");
        }
        // Install the JsonReaderInternalAccess hook now, i.e. inside the caller's
        // (single-threaded) static initialization window such as GsonUtils class init.
        // The hook field is a plain non-volatile static, so installing it lazily on
        // the first streaming read could in theory be invisible to other threads.
        EnteredObjectJsonReader.ensureInternalAccessHookInstalled();
        this.streamingDispatch = true;
        return this;
    }

    /**
     * Registers {@code type} for using when label not found
     */
    public RuntimeTypeAdapterFactory<T> registerDefaultSubtype(Class<? extends T> type) {
        if (type == null) {
            throw new NullPointerException();
        }
        defaultType = type;
        return this;
    }

    /**
     * Specifies the ${@code type} corresponding to the ${@code label}, which is used to be compatible
     * with old data when the ${@code label} of the ${@code type} changes.
     *
     * @throws IllegalArgumentException if {@code label}
     *     have already been registered on this type adapter.
     */
    public RuntimeTypeAdapterFactory<T> registerCompatibleSubtype(Class<? extends T> type, String label) {
        if (type == null || label == null) {
            throw new NullPointerException();
        }
        if (labelToSubtype.containsKey(label)) {
            throw new IllegalArgumentException("labels must be unique");
        }
        labelToSubtype.put(label, type);
        return this;
    }

    public <R> TypeAdapter<R> create(Gson gson, TypeToken<R> type) {
        if (baseType != type.getRawType() && !subtypeToLabel.containsKey(type.getRawType())
                && !(Modifier.isAbstract(type.getRawType().getModifiers())
                     && baseType.isAssignableFrom(type.getRawType()))) {
            return null;
        }

        final Map<String, TypeAdapter<?>> labelToDelegate = new LinkedHashMap<String, TypeAdapter<?>>();
        final Map<Class<?>, TypeAdapter<?>> subtypeToDelegate = new LinkedHashMap<Class<?>, TypeAdapter<?>>();
        final TypeAdapter<?> defaultDelegate = defaultType == null ? null :
                gson.getDelegateAdapter(this, TypeToken.get(defaultType));
        for (Map.Entry<String, Class<?>> entry : labelToSubtype.entrySet()) {
            TypeAdapter<?> delegate = gson.getDelegateAdapter(this, TypeToken.get(entry.getValue()));
            labelToDelegate.put(entry.getKey(), delegate);
            subtypeToDelegate.put(entry.getValue(), delegate);
        }

        return new TypeAdapter<R>() {
            @Override
            public R read(JsonReader in) throws IOException {
                if (streamingDispatch && defaultDelegate == null && in.peek() == JsonToken.BEGIN_OBJECT) {
                    return readStreaming(in);
                }
                JsonElement jsonElement = Streams.parse(in);
                JsonElement labelJsonElement;
                if (maintainType) {
                    labelJsonElement = jsonElement.getAsJsonObject().get(typeFieldName);
                } else {
                    labelJsonElement = jsonElement.getAsJsonObject().remove(typeFieldName);
                }

                if (labelJsonElement == null) {
                    if (defaultDelegate != null) {
                        // registration requires that subtype extends T
                        return (R) defaultDelegate.fromJsonTree(jsonElement);
                    }

                    throw new JsonParseException("cannot deserialize " + baseType
                            + " because it does not define a field named " + typeFieldName);
                }
                String label = labelJsonElement.getAsString();
                @SuppressWarnings("unchecked") // registration requires that subtype extends T
                TypeAdapter<R> delegate = (TypeAdapter<R>) labelToDelegate.get(label);
                if (delegate == null) {
                    throw new JsonParseException("cannot deserialize " + baseType + " subtype named " + label
                            + "; did you forget to register a subtype?");
                }
                return delegate.fromJsonTree(jsonElement);
            }

            private R readStreaming(JsonReader in) throws IOException {
                in.beginObject();
                if (!in.hasNext()) {
                    throw new JsonParseException("cannot deserialize " + baseType
                            + " because it does not define a field named " + typeFieldName);
                }
                String firstName = in.nextName();
                if (!typeFieldName.equals(firstName)) {
                    throw new JsonParseException("cannot deserialize " + baseType + " in streaming mode because"
                            + " the first field is '" + firstName + "' instead of '" + typeFieldName + "'");
                }
                String label = in.nextString();
                @SuppressWarnings("unchecked") // registration requires that subtype extends T
                TypeAdapter<R> delegate = (TypeAdapter<R>) labelToDelegate.get(label);
                if (delegate == null) {
                    throw new JsonParseException("cannot deserialize " + baseType + " subtype named " + label
                            + "; did you forget to register a subtype?");
                }
                // the type field has been consumed, equivalent to remove(typeFieldName) in tree mode
                return delegate.read(new EnteredObjectJsonReader(in));
            }

            @Override
            public void write(JsonWriter out, R value) throws IOException {
                Class<?> srcType = value.getClass();
                String label = subtypeToLabel.get(srcType);
                @SuppressWarnings("unchecked") // registration requires that subtype extends T
                TypeAdapter<R> delegate = (TypeAdapter<R>) subtypeToDelegate.get(srcType);
                if (delegate == null) {
                    throw new JsonParseException(
                            "cannot serialize " + srcType.getName() + "; did you forget to register a subtype?");
                }
                if (streamingDispatch) {
                    delegate.write(new TypeFieldInjectingJsonWriter(out, typeFieldName, label, srcType), value);
                    return;
                }
                JsonObject jsonObject = delegate.toJsonTree(value).getAsJsonObject();

                if (maintainType) {
                    Streams.write(jsonObject, out);
                    return;
                }

                JsonObject clone = new JsonObject();

                if (jsonObject.has(typeFieldName)) {
                    throw new JsonParseException("cannot serialize " + srcType.getName()
                            + " because it already defines a field named " + typeFieldName);
                }
                clone.add(typeFieldName, new JsonPrimitive(label));

                for (Map.Entry<String, JsonElement> e : jsonObject.entrySet()) {
                    clone.add(e.getKey(), e.getValue());
                }
                Streams.write(clone, out);
            }
        }.nullSafe();
    }

    /*
     * A pass-through JsonWriter which injects '"typeFieldName": "label"' right after the
     * first beginObject(), producing the same bytes as the tree mode which clones the
     * JsonObject and puts the type field first.
     */
    private static final class TypeFieldInjectingJsonWriter extends JsonWriter {
        private static final Writer UNUSED_WRITER = new Writer() {
            @Override
            public void write(char[] buffer, int offset, int counter) {
                throw new AssertionError("all methods should be forwarded to the delegate writer");
            }

            @Override
            public void flush() {
                throw new AssertionError();
            }

            @Override
            public void close() {
                throw new AssertionError();
            }
        };

        private final JsonWriter delegate;
        private final String typeFieldName;
        private final String label;
        private final Class<?> srcType;
        // depth of nested objects/arrays, used to detect the top level
        private int depth = 0;
        private boolean typeFieldWritten = false;

        TypeFieldInjectingJsonWriter(JsonWriter delegate, String typeFieldName, String label, Class<?> srcType) {
            super(UNUSED_WRITER);
            this.delegate = delegate;
            this.typeFieldName = typeFieldName;
            this.label = label;
            this.srcType = srcType;
            // these getters are final and cannot be forwarded, sync the flags instead
            setLenient(delegate.isLenient());
            setHtmlSafe(delegate.isHtmlSafe());
            setSerializeNulls(delegate.getSerializeNulls());
        }

        private void requireInsideObject() {
            if (depth == 0) {
                throw new JsonParseException("cannot serialize " + srcType.getName()
                        + " in streaming mode because it is not serialized as a json object");
            }
        }

        @Override
        public JsonWriter beginObject() throws IOException {
            delegate.beginObject();
            depth++;
            if (!typeFieldWritten) {
                typeFieldWritten = true;
                delegate.name(typeFieldName);
                delegate.value(label);
            }
            return this;
        }

        @Override
        public JsonWriter endObject() throws IOException {
            delegate.endObject();
            depth--;
            return this;
        }

        @Override
        public JsonWriter beginArray() throws IOException {
            requireInsideObject();
            delegate.beginArray();
            depth++;
            return this;
        }

        @Override
        public JsonWriter endArray() throws IOException {
            delegate.endArray();
            depth--;
            return this;
        }

        @Override
        public JsonWriter name(String name) throws IOException {
            if (depth == 1 && typeFieldName.equals(name)) {
                throw new JsonParseException("cannot serialize " + srcType.getName()
                        + " because it already defines a field named " + typeFieldName);
            }
            delegate.name(name);
            return this;
        }

        @Override
        public JsonWriter value(String value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public JsonWriter jsonValue(String value) throws IOException {
            requireInsideObject();
            delegate.jsonValue(value);
            return this;
        }

        @Override
        public JsonWriter nullValue() throws IOException {
            requireInsideObject();
            delegate.nullValue();
            return this;
        }

        @Override
        public JsonWriter value(boolean value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public JsonWriter value(Boolean value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public JsonWriter value(float value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public JsonWriter value(double value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public JsonWriter value(long value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public JsonWriter value(Number value) throws IOException {
            requireInsideObject();
            delegate.value(value);
            return this;
        }

        @Override
        public boolean isLenient() {
            return delegate.isLenient();
        }

        @Override
        public void flush() throws IOException {
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /*
     * A pass-through JsonReader for a stream whose top level BEGIN_OBJECT (and the type
     * field) has already been consumed by readStreaming(): the first beginObject() call
     * is a no-op, everything else is forwarded.
     */
    private static final class EnteredObjectJsonReader extends JsonReader {
        private static final Reader UNUSED_READER = new Reader() {
            @Override
            public int read(char[] buffer, int offset, int count) {
                throw new AssertionError("all methods should be forwarded to the delegate reader");
            }

            @Override
            public void close() {
                throw new AssertionError();
            }
        };

        static {
            // Gson's MapTypeAdapterFactory promotes a pending map key from NAME to VALUE
            // through this global hook, whose default implementation manipulates the
            // package-private state of the passed JsonReader directly instead of going
            // through the public methods. For a pass-through wrapper like
            // EnteredObjectJsonReader, the state lives in the delegate, so the hook must
            // be applied to the delegate. Replace the hook once with an unwrapping
            // version; behavior for all other JsonReader instances is unchanged.
            // (Initializing this class triggers the JsonReader static initializer first,
            // so INSTANCE is already the Gson built-in implementation here.)
            final JsonReaderInternalAccess gsonBuiltin = JsonReaderInternalAccess.INSTANCE;
            JsonReaderInternalAccess.INSTANCE = new JsonReaderInternalAccess() {
                @Override
                public void promoteNameToValue(JsonReader reader) throws IOException {
                    while (reader instanceof EnteredObjectJsonReader) {
                        reader = ((EnteredObjectJsonReader) reader).delegate;
                    }
                    gsonBuiltin.promoteNameToValue(reader);
                }
            };
        }

        /**
         * Triggers class initialization; the static block above installs the hook
         * exactly once under the JVM class-init lock.
         */
        static void ensureInternalAccessHookInstalled() {
        }

        private final JsonReader delegate;
        private boolean topObjectEntered = false;

        EnteredObjectJsonReader(JsonReader delegate) {
            super(UNUSED_READER);
            this.delegate = delegate;
            // isLenient() is final and cannot be forwarded, sync the flag instead
            setLenient(delegate.isLenient());
        }

        @Override
        public void beginObject() throws IOException {
            if (!topObjectEntered) {
                // the top level BEGIN_OBJECT has already been consumed
                topObjectEntered = true;
                return;
            }
            delegate.beginObject();
        }

        @Override
        public JsonToken peek() throws IOException {
            if (!topObjectEntered) {
                return JsonToken.BEGIN_OBJECT;
            }
            return delegate.peek();
        }

        @Override
        public void endObject() throws IOException {
            delegate.endObject();
        }

        @Override
        public void beginArray() throws IOException {
            delegate.beginArray();
        }

        @Override
        public void endArray() throws IOException {
            delegate.endArray();
        }

        @Override
        public boolean hasNext() throws IOException {
            return delegate.hasNext();
        }

        @Override
        public String nextName() throws IOException {
            return delegate.nextName();
        }

        @Override
        public String nextString() throws IOException {
            return delegate.nextString();
        }

        @Override
        public boolean nextBoolean() throws IOException {
            return delegate.nextBoolean();
        }

        @Override
        public void nextNull() throws IOException {
            delegate.nextNull();
        }

        @Override
        public double nextDouble() throws IOException {
            return delegate.nextDouble();
        }

        @Override
        public long nextLong() throws IOException {
            return delegate.nextLong();
        }

        @Override
        public int nextInt() throws IOException {
            return delegate.nextInt();
        }

        @Override
        public void skipValue() throws IOException {
            delegate.skipValue();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public String getPath() {
            return delegate.getPath();
        }

        @Override
        public String getPreviousPath() {
            return delegate.getPreviousPath();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}
