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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections.MapUtils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@Setter
public class LoadProperties {

    /**
     * 2G
     */
    @Property("exec_mem_limit")
    private long execMemLimit;

    @Property("timezone")
    private String timezone;

    @Property("load_to_single_tablet")
    private Boolean loadToSingleTablet = false;

    @Property("max_filter_ratio")
    private Double maxFilterRatio = 0.0;

    @Property("send_batch_parallelism")
    private Integer sendBatchParallelism;

    @Property(value = "timeout", needSet = true)
    private Integer timeoutS;

    /**
     * max_filter_ratio < 0 <=> isStrictMode = true
     */
    @Property(value = "strict_mode", needSet = true)
    private boolean isStrictMode;

    // ------------------------ just for routine load ------------------------

    @Property("max_batch_size")
    private Long maxBatchSize = 100 * 1024 * 1024L;

    @Property("max_batch_rows")
    private Integer maxBatchRows = 200000;

    @Property("max_batch_interval")
    private Long maxBatchIntervalS = 10L;
    // -----------------------------------------------------------------------

    private Map<String, String> properties;

    public LoadProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void analyze() {
        final ConnectContext connectContext = ConnectContext.get();
        Preconditions.checkNotNull(connectContext, "connect context should be not null");

        final SessionVariable sessionVariable = connectContext.getSessionVariable();
        final String qualifiedUser = connectContext.getQualifiedUser();
        long memLimit = Env.getCurrentEnv().getAuth().getExecMemLimit(qualifiedUser);
        this.execMemLimit = memLimit > 0 ? memLimit : sessionVariable.getMaxExecMemByte();
        final String timeZone = sessionVariable.getTimeZone();
        this.timezone = "CST".equals(timeZone) ? TimeUtils.DEFAULT_TIME_ZONE : timeZone;
        this.timeoutS = connectContext.getExecTimeout();
        this.isStrictMode = sessionVariable.getEnableInsertStrict();
        this.sendBatchParallelism = sessionVariable.getSendBatchParallelism();

        if (MapUtils.isEmpty(properties)) {
            return;
        }

        Arrays.stream(getClass().getDeclaredFields())
                .peek(field -> field.setAccessible(true))
                .filter(field -> field.isAnnotationPresent(Property.class))
                .collect(Collectors.toList())
                .forEach(field -> {
                    final Property propertyAnnotation = field.getAnnotation(Property.class);
                    final String propName = propertyAnnotation.value();
                    if (properties.containsKey(propName)) {
                        try {
                            field.set(this, parseScalar(properties.get(propName), field.getType()));
                        } catch (Exception e) {
                            throw new IllegalArgumentException(String.format("prop %s is illegal", propName), e);
                        }
                    }
                });
    }

    @SuppressWarnings("unchecked")
    private static <T> T parseScalar(String value, Class<T> targetType) {
        if (targetType == String.class) {
            return (T) value;
        } else if (targetType == Integer.class || targetType == int.class) {
            return (T) Integer.valueOf(value);
        } else if (targetType == Long.class || targetType == long.class) {
            return (T) Long.valueOf(value);
        } else if (targetType == Boolean.class || targetType == boolean.class) {
            return (T) Boolean.valueOf(value);
        } else if (targetType == Double.class || targetType == double.class) {
            return (T) Double.valueOf(value);
        } else if (targetType == Float.class || targetType == float.class) {
            return (T) Float.valueOf(value);
        } else {
            throw new IllegalArgumentException("Unsupported target type: " + targetType.getName());
        }
    }

    @Target(ElementType.FIELD)
    @Retention(RetentionPolicy.RUNTIME)
    private @interface Property {

        String value();

        /**
         * means whether the property should be set typically in each stmt
         */
        boolean needSet() default false;
    }
}
