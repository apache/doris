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

package org.apache.doris.common;

import java.io.FileReader;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfigBase {

    @Retention(RetentionPolicy.RUNTIME)
    public static @interface ConfField {
        String value() default "";
    }

    public static Properties props;
    public static Class<? extends ConfigBase> confClass;

    public void init(String propfile) throws Exception {
        props = new Properties();
        confClass = this.getClass();
        try (FileReader fr = new FileReader(propfile)) {
            props.load(fr);
        }
        replacedByEnv();
        setFields();
    }

    public static HashMap<String, String> dump() throws Exception {
        HashMap<String, String> map = new HashMap<String, String>();
        Field[] fields = confClass.getFields();
        for (Field f : fields) {
            if (f.getAnnotation(ConfField.class) == null) {
                continue;
            }
            if (f.getType().isArray()) {
                switch (f.getType().getSimpleName()) {
                    case "short[]":
                        map.put(f.getName(), Arrays.toString((short[]) f.get(null)));
                        break;
                    case "int[]":
                        map.put(f.getName(), Arrays.toString((int[]) f.get(null)));
                        break;
                    case "long[]":
                        map.put(f.getName(), Arrays.toString((long[]) f.get(null)));
                        break;
                    case "double[]":
                        map.put(f.getName(),
                                Arrays.toString((double[]) f.get(null)));
                        break;
                    case "boolean[]":
                        map.put(f.getName(),
                                Arrays.toString((boolean[]) f.get(null)));
                        break;
                    case "String[]":
                        map.put(f.getName(),
                                Arrays.toString((String[]) f.get(null)));
                        break;
                    default:
                        throw new Exception("unknown type: "
                                + f.getType().getSimpleName());
                }
            } else {
                map.put(f.getName(), f.get(null).toString());
            }
        }
        return map;
    }

    private static void replacedByEnv() throws Exception {
        Pattern pattern = Pattern.compile("\\$\\{([^\\}]*)\\}");
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            Matcher m = pattern.matcher(value);
            while (m.find()) {
                String envValue = System.getProperty(m.group(1));
                envValue = (envValue != null) ? envValue : System.getenv(m
                        .group(1));
                if (envValue != null) {
                    value = value.replace("${" + m.group(1) + "}", envValue);
                } else {
                    throw new Exception("no such env variable: " + m.group(1));
                }
            }
            props.setProperty(key, value);
        }
    }

    private static void setFields() throws Exception {
        Field[] fields = confClass.getFields();
        for (Field f : fields) {
            // ensure that field has "@ConfFiled" annotation
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno == null) {
                continue;
            }

            // ensure that field has property string
            String confStr = anno.value().equals("") ? f.getName() : anno
                    .value();
            String confVal = props.getProperty(confStr);
            if (confVal == null) {
                continue;
            }
            confVal = confVal.trim();
            String[] sa = confVal.split(",");
            for (int i = 0; i < sa.length; i++) {
                sa[i] = sa[i].trim();
            }

            // set config filed
            switch (f.getType().getSimpleName()) {
                case "short":
                    f.setShort(null, Short.parseShort(confVal));
                    break;
                case "int":
                    f.setInt(null, Integer.parseInt(confVal));
                    break;
                case "long":
                    f.setLong(null, Long.parseLong(confVal));
                    break;
                case "double":
                    f.setDouble(null, Double.parseDouble(confVal));
                    break;
                case "boolean":
                    f.setBoolean(null, Boolean.parseBoolean(confVal));
                    break;
                case "String":
                    f.set(null, confVal);
                    break;
                case "short[]":
                    short[] sha = new short[sa.length];
                    for (int i = 0; i < sha.length; i++) {
                        sha[i] = Short.parseShort(sa[i]);
                    }
                    f.set(null, sha);
                    break;
                case "int[]":
                    int[] ia = new int[sa.length];
                    for (int i = 0; i < ia.length; i++) {
                        ia[i] = Integer.parseInt(sa[i]);
                    }
                    f.set(null, ia);
                    break;
                case "long[]":
                    long[] la = new long[sa.length];
                    for (int i = 0; i < la.length; i++) {
                        la[i] = Long.parseLong(sa[i]);
                    }
                    f.set(null, la);
                    break;
                case "double[]":
                    double[] da = new double[sa.length];
                    for (int i = 0; i < da.length; i++) {
                        da[i] = Double.parseDouble(sa[i]);
                    }
                    f.set(null, da);
                    break;
                case "boolean[]":
                    boolean[] ba = new boolean[sa.length];
                    for (int i = 0; i < ba.length; i++) {
                        ba[i] = Boolean.parseBoolean(sa[i]);
                    }
                    f.set(null, ba);
                    break;
                case "String[]":
                    f.set(null, sa);
                    break;
                default:
                    throw new Exception("unknown type: "
                            + f.getType().getSimpleName());
            }
        }
    }

}
