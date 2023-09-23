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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConfigBase {
    private static final Logger LOG = LogManager.getLogger(ConfigBase.class);

    @Retention(RetentionPolicy.RUNTIME)
    public @interface ConfField {
        boolean mutable() default false;

        boolean masterOnly() default false;

        String comment() default "";

        VariableAnnotation varType() default VariableAnnotation.NONE;

        Class<? extends ConfHandler> callback() default DefaultConfHandler.class;

        // description for this config item.
        // There should be 2 elements in the array.
        // The first element is the description in Chinese.
        // The second element is the description in English.
        String[] description() default {"待补充", "TODO"};

        // Enum options for this config item, if it has.
        String[] options() default {};
    }

    public interface ConfHandler {
        void handle(Field field, String confVal) throws Exception;
    }

    static class DefaultConfHandler implements ConfHandler {
        @Override
        public void handle(Field field, String confVal) throws Exception {
            setConfigField(field, confVal);
        }
    }

    private static String confFile;
    private static String customConfFile;
    public static Class<? extends ConfigBase> confClass;
    public static Map<String, Field> confFields;

    private static String ldapConfFile;
    private static String ldapCustomConfFile;
    public static Class<? extends ConfigBase> ldapConfClass;

    public static Map<String, Field> ldapConfFields = Maps.newHashMap();

    private boolean isLdapConfig = false;

    public void init(String configFile) throws Exception {
        if (this instanceof Config) {
            confClass = this.getClass();
            confFile = configFile;
            confFields = Maps.newHashMap();
            for (Field field : confClass.getFields()) {
                ConfField confField = field.getAnnotation(ConfField.class);
                if (confField == null) {
                    continue;
                }
                confFields.put(field.getName(), field);
                confFields.put(confField.varType().getPrefix() + field.getName(), field);
            }

            initConf(confFile);
        } else if (this instanceof LdapConfig) {
            isLdapConfig = true;
            ldapConfClass = this.getClass();
            ldapConfFile = configFile;
            for (Field field : ldapConfClass.getFields()) {
                ConfField confField = field.getAnnotation(ConfField.class);
                if (confField == null) {
                    continue;
                }
                ldapConfFields.put(field.getName(), field);
                ldapConfFields.put(confField.varType().getPrefix() + field.getName(), field);
            }
            initConf(ldapConfFile);
        }
    }

    public void initCustom(String customConfFile) throws Exception {
        this.customConfFile = customConfFile;
        File file = new File(customConfFile);
        if (file.exists() && file.isFile()) {
            // customConfFile is introduced in version 0.14, for compatibility, check if it exist
            // config in customConfFile will overwrite the config in confFile
            initConf(customConfFile);
        }
    }

    private void initConf(String confFile) throws Exception {
        Properties props = new Properties();
        try (FileReader fr = new FileReader(confFile)) {
            props.load(fr);
        }
        replacedByEnv(props);
        setFields(props, isLdapConfig);
    }

    public static HashMap<String, String> dump() {
        HashMap<String, String> map = new HashMap<>();
        Field[] fields = confClass.getFields();
        for (Field f : fields) {
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno != null) {
                map.put(f.getName(), getConfValue(f));
            }
        }
        return map;
    }

    public static String getConfValue(Field field) {
        try {
            if (field.getType().isArray()) {
                switch (field.getType().getSimpleName()) {
                    case "boolean[]":
                        return Arrays.toString((boolean[]) field.get(null));
                    case "char[]":
                        return Arrays.toString((char[]) field.get(null));
                    case "byte[]":
                        return Arrays.toString((byte[]) field.get(null));
                    case "short[]":
                        return Arrays.toString((short[]) field.get(null));
                    case "int[]":
                        return Arrays.toString((int[]) field.get(null));
                    case "long[]":
                        return Arrays.toString((long[]) field.get(null));
                    case "float[]":
                        return Arrays.toString((float[]) field.get(null));
                    case "double[]":
                        return Arrays.toString((double[]) field.get(null));
                    default:
                        return Arrays.toString((Object[]) field.get(null));
                }
            } else {
                return String.valueOf(field.get(null));
            }
        } catch (Exception e) {
            return String.format("Failed to get config %s: %s", field.getName(), e.getMessage());
        }
    }

    // there is some config in fe.conf like:
    // config_key={CONFIG_VALUE}
    // the "CONFIG_VALUE" should be replaced be env variable CONFIG_VALUE
    private void replacedByEnv(Properties props) throws Exception {
        // pattern to match string like "{CONFIG_VALUE}"
        Pattern pattern = Pattern.compile("\\$\\{([^}]*)\\}");
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            Matcher m = pattern.matcher(value);
            while (m.find()) {
                String envValue = System.getProperty(m.group(1));
                envValue = (envValue != null) ? envValue : System.getenv(m.group(1));
                if (envValue != null) {
                    value = value.replace("${" + m.group(1) + "}", envValue);
                } else {
                    throw new Exception("no such env variable: " + m.group(1));
                }
            }
            props.setProperty(key, value);
        }
    }

    private static void setFields(Properties props, boolean isLdapConfig) throws Exception {
        Class<? extends ConfigBase> theClass = isLdapConfig ? ldapConfClass : confClass;
        Field[] fields = theClass.getFields();
        for (Field f : fields) {
            // ensure that field has "@ConfField" annotation
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno == null) {
                continue;
            }

            // ensure that field has property string
            String confKey = f.getName();
            String confVal = props.getProperty(confKey, props.getProperty(anno.varType().getPrefix() + confKey));
            if (Strings.isNullOrEmpty(confVal)) {
                continue;
            }

            setConfigField(f, confVal);
        }
    }

    private static void setConfigField(Field f, String confVal) throws Exception {
        confVal = confVal.trim();

        String[] sa = confVal.split(",");
        for (int i = 0; i < sa.length; i++) {
            sa[i] = sa[i].trim();
        }

        // set config field
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
                if (isBoolean(confVal)) {
                    f.setBoolean(null, Boolean.parseBoolean(confVal));
                }
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
                    if (isBoolean(sa[i])) {
                        ba[i] = Boolean.parseBoolean(sa[i]);
                    }
                }
                f.set(null, ba);
                break;
            case "String[]":
                f.set(null, sa);
                break;
            default:
                throw new Exception("unknown type: " + f.getType().getSimpleName());
        }
    }

    private static boolean isBoolean(String s) {
        if (s.equalsIgnoreCase("true") || s.equalsIgnoreCase("false")) {
            return true;
        }
        throw new IllegalArgumentException("type mismatch");
    }

    public static synchronized void setMutableConfig(String key, String value) throws ConfigException {
        Field field = confFields.get(key);
        if (field == null) {
            if (ldapConfFields.containsKey(key)) {
                field = ldapConfFields.get(key);
            } else {
                throw new ConfigException("Config '" + key + "' does not exist");
            }
        }

        ConfField anno = field.getAnnotation(ConfField.class);
        if (!anno.mutable()) {
            throw new ConfigException("Config '" + key + "' is not mutable");
        }

        try {
            anno.callback().newInstance().handle(field, value);
        } catch (Exception e) {
            throw new ConfigException("Failed to set config '" + key + "'. err: " + e.getMessage());
        }

        LOG.info("set config {} to {}", key, value);
    }

    /**
     * Get display name of experimental configs.
     * For an experimental/deprecated config, the given "configsToFilter" contains both config w/o
     * "experimental_/deprecated_" prefix.
     *
     * @param configsToFilter
     * @param allConfigs
     */
    private static void getDisplayConfigInfo(Map<String, Field> configsToFilter, Map<String, Field> allConfigs) {
        for (Map.Entry<String, Field> e : configsToFilter.entrySet()) {
            Field f = e.getValue();
            ConfField confField = f.getAnnotation(ConfField.class);

            if (!e.getKey().startsWith(confField.varType().getPrefix())) {
                continue;
            }
            allConfigs.put(e.getKey(), f);
        }
    }

    public static synchronized List<List<String>> getConfigInfo(PatternMatcher matcher) {
        Map<String, Field> allConfFields = Maps.newHashMap();
        getDisplayConfigInfo(confFields, allConfFields);
        getDisplayConfigInfo(ldapConfFields, allConfFields);

        return allConfFields.entrySet().stream().sorted(Map.Entry.comparingByKey()).flatMap(e -> {
            String confKey = e.getKey();
            Field f = e.getValue();
            ConfField confField = f.getAnnotation(ConfField.class);
            if (matcher == null || matcher.match(confKey)) {
                List<String> config = Lists.newArrayList();
                config.add(confKey);
                config.add(getConfValue(f));
                config.add(f.getType().getSimpleName());
                config.add(String.valueOf(confField.mutable()));
                config.add(String.valueOf(confField.masterOnly()));
                config.add(confField.comment());
                return Stream.of(config);
            } else {
                return Stream.empty();
            }
        }).collect(Collectors.toList());
    }

    public static synchronized boolean checkIsMasterOnly(String key) {
        Field f = confFields.get(key);
        if (f == null) {
            return false;
        }

        ConfField anno = f.getAnnotation(ConfField.class);
        return anno != null && anno.mutable() && anno.masterOnly();
    }

    // use synchronized to make sure only one thread modify this file
    public static synchronized void persistConfig(Map<String, String> customConf, boolean resetPersist)
            throws IOException {
        File file = new File(customConfFile);
        if (!file.exists()) {
            file.createNewFile();
        } else if (resetPersist) {
            // clear the customConfFile content
            try (PrintWriter writer = new PrintWriter(file)) {
                writer.print("");
            }
        }

        Properties props = new Properties();
        try (FileReader fr = new FileReader(customConfFile)) {
            props.load(fr);
        }

        for (Map.Entry<String, String> entry : customConf.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        try (FileOutputStream fos = new FileOutputStream(file)) {
            props.store(fos, "THIS IS AN AUTO GENERATED CONFIG FILE.\n"
                    + "You can modify this file manually, and the configurations in this file\n"
                    + "will overwrite the configurations in fe.conf");
        }
    }

    public static int getConfigNumByVariableAnnotation(VariableAnnotation type) {
        int num = 0;
        for (Field field : Config.class.getFields()) {
            ConfField confField = field.getAnnotation(ConfField.class);
            if (confField == null) {
                continue;
            }
            if (confField.varType() == type) {
                ++num;
            }
        }
        return num;
    }
}
