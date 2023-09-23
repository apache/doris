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

package org.apache.doris.common.property;

import org.apache.doris.common.io.Text;
import org.apache.doris.thrift.TPropertyVal;

import com.google.common.collect.ImmutableMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;

@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class PropertySchema<T> {
    private final String name;
    private final boolean required;
    private Optional<T> defaultValue = Optional.empty();
    private Optional<T> maxValue = Optional.empty();
    private Optional<T> minValue = Optional.empty();

    protected PropertySchema(String name) {
        this(name, false);
    }

    public PropertySchema(String name, boolean required) {
        this.name = name;
        this.required = required;
    }

    public static ImmutableMap<String, PropertySchema> createSchemas(PropertySchema ... schemas) {
        ImmutableMap.Builder builder = ImmutableMap.builder();
        Arrays.stream(schemas).forEach(s -> builder.put(s.getName(), s));
        return builder.build();
    }

    public interface SchemaGroup {
        ImmutableMap<String, PropertySchema> getSchemas();
    }

    public static final class StringProperty extends ComparableProperty<String> {
        StringProperty(String name) {
            super(name);
        }

        StringProperty(String name, boolean isRequired) {
            super(name, isRequired);
        }

        @Override
        public String read(String rawVal) throws IllegalArgumentException {
            verifyRange(rawVal);
            return rawVal;
        }

        @Override
        public String read(TPropertyVal tVal) throws IllegalArgumentException {
            verifyRange(tVal.getStrVal());
            return tVal.getStrVal();
        }

        @Override
        public String read(DataInput input) throws IOException {
            return Text.readString(input);
        }

        @Override
        public void write(String val, TPropertyVal out) {
            out.setStrVal(val);
        }

        @Override
        public void write(String val, DataOutput out) throws IOException {
            Text.writeString(out, val);
        }
    }

    public static final class IntProperty extends ComparableProperty<Integer> {
        IntProperty(String name) {
            super(name);
        }

        IntProperty(String name, boolean isRequired) {
            super(name, isRequired);
        }

        @Override
        public Integer read(String rawVal) {
            try {
                Integer val = Integer.parseInt(rawVal);
                verifyRange(val);
                return val;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("Invalid integer %s: %s", rawVal, e.getMessage()));
            }
        }

        @Override
        public Integer read(TPropertyVal tVal) throws IllegalArgumentException {
            verifyRange(tVal.getIntVal());
            return tVal.getIntVal();
        }

        @Override
        public Integer read(DataInput input) throws IOException {
            return input.readInt();
        }

        @Override
        public void write(Integer val, TPropertyVal out) {
            out.setIntVal(val);
        }

        @Override
        public void write(Integer val, DataOutput out) throws IOException {
            out.writeInt(val);
        }
    }

    public static final class LongProperty extends ComparableProperty<Long> {
        LongProperty(String name) {
            super(name);
        }

        LongProperty(String name, boolean isRequired) {
            super(name, isRequired);
        }

        @Override
        public Long read(String rawVal) {
            try {
                Long val = Long.parseLong(rawVal);
                verifyRange(val);
                return val;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("Invalid long %s: %s", rawVal, e.getMessage()));
            }
        }

        @Override
        public Long read(TPropertyVal tVal) throws IllegalArgumentException {
            verifyRange(tVal.getLongVal());
            return tVal.getLongVal();
        }

        @Override
        public Long read(DataInput input) throws IOException {
            return input.readLong();
        }

        @Override
        public void write(Long val, TPropertyVal out) {
            out.setLongVal(val);
        }

        @Override
        public void write(Long val, DataOutput out) throws IOException {
            out.writeLong(val);
        }
    }

    public static final class BooleanProperty extends ComparableProperty<Boolean> {
        BooleanProperty(String name) {
            super(name);
        }

        BooleanProperty(String name, boolean isRequired) {
            super(name, isRequired);
        }

        @Override
        public Boolean read(String rawVal) {
            if (rawVal == null || (!rawVal.equalsIgnoreCase("true")
                    && !rawVal.equalsIgnoreCase("false"))) {
                throw new IllegalArgumentException(String.format("Invalid boolean : %s, use true or false", rawVal));
            }

            try {
                return Boolean.parseBoolean(rawVal);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format("Invalid boolean %s: %s", rawVal, e.getMessage()));
            }
        }

        @Override
        public Boolean read(TPropertyVal tVal) throws IllegalArgumentException {
            return tVal.isBoolVal();
        }

        @Override
        public Boolean read(DataInput input) throws IOException {
            return input.readBoolean();
        }

        @Override
        public void write(Boolean val, TPropertyVal out) {
            out.setBoolVal(val);
        }

        @Override
        public void write(Boolean val, DataOutput out) throws IOException {
            out.writeBoolean(val);
        }
    }

    public static final class DateProperty extends PropertySchema<Date> {
        DateTimeFormatter dateFormat;

        public DateProperty(String name, DateTimeFormatter dateFormat) {
            super(name);
            this.dateFormat = dateFormat;
        }

        DateProperty(String name, DateTimeFormatter dateFormat, boolean isRequired) {
            super(name, isRequired);
            this.dateFormat = dateFormat;
        }

        @Override
        public Date read(String rawVal) throws IllegalArgumentException {
            if (rawVal == null) {
                throw new IllegalArgumentException("Invalid time format, time param can not is null");
            }
            return readTimeFormat(rawVal);
        }

        @Override
        public Date read(TPropertyVal tVal) throws IllegalArgumentException {
            return readTimeFormat(tVal.getStrVal());
        }

        @Override
        public Date read(DataInput input) throws IOException {
            return readTimeFormat(Text.readString(input));
        }

        @Override
        public void write(Date val, TPropertyVal out) {
            out.setStrVal(writeTimeFormat(val));
        }

        @Override
        public void write(Date val, DataOutput out) throws IOException {
            Text.writeString(out, writeTimeFormat(val));
        }

        public Date readTimeFormat(String timeStr) throws IllegalArgumentException {
            try {
                return Date.from(LocalDateTime.parse(timeStr, dateFormat).atZone(ZoneId.systemDefault()).toInstant());
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid time format, time param need "
                        + "to be " + this.dateFormat.toString());
            }
        }

        public String writeTimeFormat(Date timeDate) throws IllegalArgumentException {
            return LocalDateTime.ofInstant(timeDate.toInstant(), ZoneId.systemDefault()).format(this.dateFormat);
        }
    }

    public static final class EnumProperty<T extends Enum<T>> extends PropertySchema<T> {
        private final Class<T> enumClass;

        EnumProperty(String name, Class<T> enumClass) {
            super(name);
            this.enumClass = enumClass;
        }

        EnumProperty(String name, Class<T> enumClass, boolean isRequired) {
            super(name, isRequired);
            this.enumClass = enumClass;
        }

        @Override
        public T read(String rawVal) {
            if (rawVal == null || rawVal.length() == 0) {
                throw new IllegalArgumentException(formatError(rawVal));
            }
            try {
                return T.valueOf(enumClass, rawVal.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(formatError(rawVal));
            }
        }

        @Override
        public T read(TPropertyVal tVal) throws IllegalArgumentException {
            return T.valueOf(enumClass, tVal.getStrVal());
        }

        @Override
        public T read(DataInput input) throws IOException {
            return T.valueOf(enumClass, Text.readString(input));
        }

        @Override
        public void write(T val, TPropertyVal out) {
            out.setStrVal(val.name());
        }

        @Override
        public void write(T val, DataOutput out) throws IOException {
            Text.writeString(out, val.name());
        }

        private String formatError(String rawVal) {
            String enumsStr = Arrays.stream(enumClass.getEnumConstants())
                    .map(Enum::toString)
                    .reduce((sa, sb) -> sa + ", " + sb)
                    .orElse("");
            return String.format("Expected values are [%s], while [%s] provided", enumsStr, rawVal);
        }
    }

    private abstract static class ComparableProperty<T extends Comparable> extends PropertySchema<T> {
        protected ComparableProperty(String name) {
            super(name);
        }

        protected ComparableProperty(String name, boolean isRequired) {
            super(name, isRequired);
        }

        protected void verifyRange(T val) throws IllegalArgumentException {
            if (getMinValue().isPresent() && (val == null || getMinValue().get().compareTo(val) > 0)) {
                throw new IllegalArgumentException(val + " should not be less than " + getMinValue().get());
            }

            if (getMaxValue().isPresent() && (val == null || getMaxValue().get().compareTo(val) < 0)) {
                throw new IllegalArgumentException(val + " should not be greater than " + getMaxValue().get());
            }
        }
    }

    PropertySchema<T> setDefauleValue(T val) {
        this.defaultValue = Optional.of(val);
        return this;
    }

    PropertySchema<T> setMin(T min) {
        this.minValue = Optional.of(min);
        return this;
    }

    PropertySchema<T> setMax(T max) {
        this.maxValue = Optional.of(max);
        return this;
    }

    public String getName() {
        return name;
    }

    public boolean isRequired() {
        return required;
    }

    public Optional<T> getDefaultValue() {
        return defaultValue;
    }

    public Optional<T> getMinValue() {
        return minValue;
    }

    public Optional<T> getMaxValue() {
        return maxValue;
    }

    public abstract T read(String rawVal) throws IllegalArgumentException;

    public abstract T read(TPropertyVal tVal) throws IllegalArgumentException;

    public abstract T read(DataInput input) throws IOException;

    public abstract void write(T val, TPropertyVal out);

    public abstract void write(T val, DataOutput out) throws IOException;
}
