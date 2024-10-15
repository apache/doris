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

package org.apache.doris.common.util;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase.ConfField;
import org.apache.doris.common.LogUtils;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

/**
 * This class is used to generate doc for FE config and session variable.
 * The doc is generated from Config.java and SessionVariable.java
 */
@Slf4j
public class DocGenerator {
    private static final String PLACEHOLDER = "<--DOC_PLACEHOLDER-->";
    private static final String[] TYPE = new String[] {"类型：", "Type: "};
    private static final String[] DEFAULT_VALYUE = new String[] {"默认值：", "Default: "};
    private static final String[] OPTIONS = new String[] {"可选值：", "Options: "};
    private static final String[] CONF_MUTABLE = new String[] {"是否可动态修改：", "Mutable: "};
    private static final String[] CONF_MASTER_ONLY = new String[] {"是否为 Master FE 节点独有的配置项：",
            "Master only: "};
    private static final String[] VAR_READ_ONLY = new String[] {"只读变量：", "Read Only: "};
    private static final String[] VAR_GLOBAL_ONLY = new String[] {"仅全局变量：", "Global only: "};


    private String configDocTemplatePath;
    private String configDocTemplatePathCN;
    private String configDocOutputPath;
    private String configDocOutputPathCN;
    private String sessionVariableDocTemplatePath;
    private String sessionVariableDocTemplatePathCN;
    private String sessionVariableDocOutputPath;
    private String sessionVariableDocOutputPathCN;

    private enum Lang {
        CN(0),
        EN(1);

        private int idx;

        Lang(int idx) {
            this.idx = idx;
        }
    }

    public DocGenerator(String configDocTemplatePath, String configDocTemplatePathCN,
            String configDocOutputPath, String configDocOutputPathCN,
            String sessionVariableDocTemplatePath, String sessionVariableDocTemplatePathCN,
            String sessionVariableDocOutputPath, String sessionVariableDocOutputPathCN) {
        this.configDocTemplatePath = configDocTemplatePath;
        this.configDocTemplatePathCN = configDocTemplatePathCN;
        this.configDocOutputPath = configDocOutputPath;
        this.configDocOutputPathCN = configDocOutputPathCN;
        this.sessionVariableDocOutputPath = sessionVariableDocOutputPath;
        this.sessionVariableDocTemplatePathCN = sessionVariableDocTemplatePathCN;
        this.sessionVariableDocTemplatePath = sessionVariableDocTemplatePath;
        this.sessionVariableDocOutputPathCN = sessionVariableDocOutputPathCN;
    }

    public void generate() throws Exception {
        generateConfigDoc();
        generateSessionVariableDoc();
    }

    private void generateConfigDoc() throws Exception {
        // 1. CN
        String contentCN = readDocTemplate(this.configDocTemplatePathCN);
        contentCN = contentCN.replace(PLACEHOLDER, genFEConfigDoc(Lang.CN));
        // 2. EN
        String content = readDocTemplate(this.configDocTemplatePath);
        content = content.replace(PLACEHOLDER, genFEConfigDoc(Lang.EN));
        // 3. write CN
        writeDoc(contentCN, this.configDocOutputPathCN);
        // 4. write EN
        writeDoc(content, this.configDocOutputPath);
    }

    private void generateSessionVariableDoc() throws Exception {
        // 1. CN
        String contentCN = readDocTemplate(this.sessionVariableDocTemplatePathCN);
        contentCN = contentCN.replace(PLACEHOLDER, genSessionVariableDoc(Lang.CN));
        // 2. EN
        String content = readDocTemplate(this.sessionVariableDocTemplatePath);
        content = content.replace(PLACEHOLDER, genSessionVariableDoc(Lang.EN));
        // 3. write CN
        writeDoc(contentCN, this.sessionVariableDocOutputPathCN);
        // 4. write EN
        writeDoc(content, this.sessionVariableDocOutputPath);
    }

    private String readDocTemplate(String templatePath) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(templatePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }

    private void writeDoc(String content, String outputPath) throws Exception {
        try (BufferedWriter bw = new BufferedWriter(new FileWriterWithEncoding(outputPath, StandardCharsets.UTF_8))) {
            bw.write(content);
        }
    }

    // generate doc for FE configs.
    // Content will be sorted by config name.
    private String genFEConfigDoc(Lang lang) throws IllegalAccessException {
        Map<String, String> sortedDoc = Maps.newTreeMap();
        Class confClass = Config.class;
        for (Field field : confClass.getFields()) {
            try {
                String res = genSingleConfFieldDoc(field, lang);
                if (!Strings.isNullOrEmpty(res)) {
                    sortedDoc.put(field.getName(), res);
                }
            } catch (Exception e) {
                LogUtils.stderr("Failed to generate doc for field: " + field.getName());
                throw e;
            }
        }
        return printSortedMap(sortedDoc);
    }

    // Generate doc for a single config field.
    private String genSingleConfFieldDoc(Field field, Lang lang) throws IllegalAccessException {
        StringBuilder sb = new StringBuilder();
        ConfField confField = field.getAnnotation(ConfField.class);
        if (confField == null) {
            return null;
        }
        String configName = confField.varType().getPrefix() + field.getName();
        sb.append("### `").append(configName).append("`\n\n");
        sb.append(confField.description()[lang.idx]).append("\n\n");
        sb.append(TYPE[lang.idx]).append("`").append(field.getType().getSimpleName()).append("`\n\n");
        sb.append(DEFAULT_VALYUE[lang.idx]).append("`").append(getStringValue(field, null)).append("`\n\n");
        if (confField.options().length > 0) {
            sb.append(OPTIONS[lang.idx]);
            for (int i = 0; i < confField.options().length; i++) {
                sb.append("`").append(confField.options()[i]).append("`");
                if (i != confField.options().length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("\n\n");
        }
        sb.append(CONF_MUTABLE[lang.idx]).append("`").append(confField.mutable()).append("`\n\n");
        sb.append(CONF_MASTER_ONLY[lang.idx]).append("`").append(confField.masterOnly()).append("`\n\n");
        return sb.toString();
    }

    private static String getStringValue(Field field, Object instance) throws IllegalAccessException {
        if (field.getType().isArray()) {
            return Arrays.toString((Object[]) field.get(instance));
        } else {
            return String.valueOf(field.get(instance));
        }
    }

    // generate doc for Session Variables
    // Content will be sorted by variables' name.
    private String genSessionVariableDoc(Lang lang) throws IllegalAccessException {
        Map<String, String> sortedDoc = Maps.newTreeMap();
        // 1. session variables
        SessionVariable sv = new SessionVariable();
        Class svClass = SessionVariable.class;
        for (Field field : svClass.getFields()) {
            try {
                String res = genSingleSessionVariableDoc(sv, field, lang);
                if (!Strings.isNullOrEmpty(res)) {
                    sortedDoc.put(field.getAnnotation(VariableMgr.VarAttr.class).name(), res);
                }
            } catch (Exception e) {
                LogUtils.stderr("Failed to generate doc for " + field.getName());
                throw e;
            }
        }
        // 2. global variables
        Class gvClass = GlobalVariable.class;
        for (Field field : gvClass.getFields()) {
            try {
                String res = genSingleSessionVariableDoc(null, field, lang);
                if (!Strings.isNullOrEmpty(res)) {
                    sortedDoc.put(field.getAnnotation(VariableMgr.VarAttr.class).name(), res);
                }
            } catch (Exception e) {
                LogUtils.stderr("Failed to generate doc for field: " + field.getName());
                throw e;
            }
        }
        return printSortedMap(sortedDoc);
    }

    @NotNull
    private static String printSortedMap(Map<String, String> sortedDoc) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : sortedDoc.entrySet()) {
            sb.append(entry.getValue());
        }
        return sb.toString();
    }

    private String genSingleSessionVariableDoc(SessionVariable sv, Field field, Lang lang)
            throws IllegalAccessException {
        VariableMgr.VarAttr varAttr = field.getAnnotation(VariableMgr.VarAttr.class);
        if (varAttr == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        String varName = varAttr.varType().getPrefix() + varAttr.name();
        sb.append("### `").append(varName).append("`\n\n");
        sb.append(varAttr.description()[lang.idx]).append("\n\n");
        sb.append(TYPE[lang.idx]).append("`").append(field.getType().getSimpleName()).append("`\n\n");
        sb.append(DEFAULT_VALYUE[lang.idx]).append("`").append(getStringValue(field, sv)).append("`\n\n");
        if (varAttr.options().length > 0) {
            sb.append(OPTIONS[lang.idx]);
            for (int i = 0; i < varAttr.options().length; i++) {
                sb.append("`").append(varAttr.options()[i]).append("`");
                if (i != varAttr.options().length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("\n\n");
        }
        sb.append(VAR_READ_ONLY[lang.idx]).append("`").append(varAttr.flag() == VariableMgr.READ_ONLY).append("`\n\n");
        sb.append(VAR_GLOBAL_ONLY[lang.idx]).append("`").append(varAttr.flag() == VariableMgr.GLOBAL).append("`\n\n");
        return sb.toString();
    }

    /**
     * generate config and session variable doc from given templates
     *
     * @param args args[0]: config doc template path
     * args[1]: config doc template path CN
     * args[2]: config doc output path
     * args[3]: config doc output path CN
     * args[4]: session variable doc template path
     * args[5]: session variable doc template path CN
     * args[6]: session variable doc output path
     * args[7]: session variable doc output path CN
     */
    public static void main(String[] args) {
        String configDocTemplatePath = args[0];
        String configDocTemplatePathCN = args[1];
        String configDocOutputPath = args[2];
        String configDocOutputPathCN = args[3];
        String sessionVariableDocTemplatePath = args[4];
        String sessionVariableDocTemplatePathCN = args[5];
        String sessionVariableDocOutputPath = args[6];
        String sessionVariableDocOutputPathCN = args[7];
        DocGenerator docGenerator = new DocGenerator(
                configDocTemplatePath, configDocTemplatePathCN,
                configDocOutputPath, configDocOutputPathCN,
                sessionVariableDocTemplatePath, sessionVariableDocTemplatePathCN,
                sessionVariableDocOutputPath, sessionVariableDocOutputPathCN);
        try {
            docGenerator.generate();
            LogUtils.stdout("Done!");
        } catch (Exception e) {
            log.info("failed to generate doc", e);
            System.exit(-1);
        }
    }
}
