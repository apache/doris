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

package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.JavaLexer;
import org.apache.doris.nereids.JavaParser;
import org.apache.doris.nereids.pattern.generator.javaast.TypeDeclaration;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Standalone code generator for the nereids pattern interfaces.
 *
 * <p>It scans the nereids sources, derives the plan and expression inheritance relations from the
 * parsed java ast, and writes:
 * <ul>
 *     <li>{@code GeneratedPlanRelations} and {@code GeneratedExpressionRelations}, the type relations
 *     used by pattern matching,</li>
 *     <li>{@code GeneratedPlanPatterns} and {@code GeneratedMemoPatterns}, the interfaces which provide
 *     the {@code logicalFilter()} style pattern factories used by the rules.</li>
 * </ul>
 *
 * <p>It replaces the former {@code PatternDescribableProcessor} annotation processor. Running it as a
 * plain program instead of as an annotation processor keeps the generated files out of the javac
 * round, and lets the generator:
 * <ul>
 *     <li>reuse the ast of the unchanged sources via {@link PatternGeneratorCache},</li>
 *     <li>rewrite an output file only when its content really changed, so that the timestamps of the
 *     generated sources stay stable and they stop invalidating the incremental compilation of the
 *     whole module.</li>
 * </ul>
 */
public class PatternCodeGenerator {
    private static final String GENERATED_PACKAGE_PATH = "org/apache/doris/nereids/pattern";

    /** entry point, see the {@code generate-patterns} execution in fe-core/pom.xml. */
    public static void main(String[] args) throws Exception {
        long startNanos = System.nanoTime();
        Map<String, String> options = parseOptions(args);
        List<File> sourceDirs = splitPaths(requireOption(options, "path"));
        File outputDir = new File(requireOption(options, "output"));
        File cacheFile = new File(requireOption(options, "cache"));

        List<File> javaFiles = findJavaFiles(sourceDirs);
        PatternGeneratorCache cache = PatternGeneratorCache.load(cacheFile);

        JavaAstAnalyzer analyzer = new JavaAstAnalyzer();
        int parsed = 0;
        for (File javaFile : javaFiles) {
            String path = javaFile.getAbsolutePath();
            byte[] contentHash = PatternGeneratorCache.contentHash(javaFile);
            List<TypeDeclaration> declarations = cache.getDeclarations(path, contentHash);
            if (declarations == null) {
                declarations = parseJavaFile(javaFile);
                cache.putDeclarations(path, contentHash, declarations);
                parsed++;
            }
            analyzer.addAsts(declarations);
        }
        cache.retain(javaFiles);
        analyzer.analyze();

        Map<String, String> generatedCodes = new LinkedHashMap<>();
        generatedCodes.put("GeneratedExpressionRelations",
                new ExpressionTypeMappingGenerator(analyzer).generateCode());
        generatedCodes.put("GeneratedPlanRelations",
                new PlanTypeMappingGenerator(analyzer).generateCode());

        PlanPatternGeneratorAnalyzer patternGeneratorAnalyzer = new PlanPatternGeneratorAnalyzer(analyzer);
        generatedCodes.put("GeneratedMemoPatterns",
                patternGeneratorAnalyzer.generatePatterns("GeneratedMemoPatterns", "MemoPatterns", true));
        generatedCodes.put("GeneratedPlanPatterns",
                patternGeneratorAnalyzer.generatePatterns("GeneratedPlanPatterns", "PlanPatterns", false));

        int rewritten = 0;
        for (Map.Entry<String, String> generatedCode : generatedCodes.entrySet()) {
            File outputFile = new File(outputDir,
                    GENERATED_PACKAGE_PATH + "/" + generatedCode.getKey() + ".java");
            if (writeIfContentChanged(outputFile, generatedCode.getValue())) {
                rewritten++;
            }
        }

        cache.save(cacheFile);

        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
        System.out.println("[pattern-generator] " + javaFiles.size() + " source files, " + parsed
                + " parsed, " + (javaFiles.size() - parsed) + " reused from cache, " + rewritten + "/"
                + generatedCodes.size() + " generated files rewritten, " + elapsedMillis + " ms");
    }

    /**
     * Write the file only when its content differs, so that an unchanged generated source keeps its
     * timestamp and does not invalidate the incremental compilation of the module which consumes it.
     */
    private static boolean writeIfContentChanged(File file, String code) throws IOException {
        byte[] content = code.getBytes(StandardCharsets.UTF_8);
        if (file.isFile() && Arrays.equals(Files.readAllBytes(file.toPath()), content)) {
            return false;
        }
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs() && !parent.exists()) {
            throw new IOException("Can not create directory " + parent);
        }
        Files.write(file.toPath(), content);
        return true;
    }

    private static List<File> findJavaFiles(List<File> dirs) {
        List<File> files = new ArrayList<>();
        for (File dir : dirs) {
            files.addAll(FileUtils.listFiles(dir, new String[] {"java"}, true));
        }
        // The directory listing order is not stable, but the generated code must be, otherwise the
        // outputs would differ from build to build and would be rewritten every time.
        files.sort(Comparator.comparing(File::getAbsolutePath));
        return files;
    }

    private static List<TypeDeclaration> parseJavaFile(File javaFile) throws IOException {
        String javaCodeString = FileUtils.readFileToString(javaFile, StandardCharsets.UTF_8);
        JavaLexer lexer = new JavaLexer(CharStreams.fromString(javaCodeString));

        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        JavaParser parser = new JavaParser(tokenStream);
        parser.setErrorHandler(new DefaultErrorStrategy() {
            @Override
            public Token recoverInline(Parser recognizer) throws RecognitionException {
                if (nextTokensContext == null) {
                    throw new InputMismatchException(recognizer);
                } else {
                    throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                }
            }
        });

        ParserRuleContext tree;
        try {
            // first, try parsing with potentially faster SLL mode
            parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
            tree = parser.compilationUnit();
        } catch (ParseCancellationException ex) {
            // if we fail, parse with LL mode
            tokenStream.seek(0); // rewind input stream
            parser.reset();

            parser.getInterpreter().setPredictionMode(PredictionMode.LL);
            tree = parser.compilationUnit();
        }

        return new JavaAstBuilder().build(tree);
    }

    private static Map<String, String> parseOptions(String[] args) {
        Map<String, String> options = new LinkedHashMap<>();
        for (String arg : args) {
            int separator = arg.startsWith("--") ? arg.indexOf('=') : -1;
            if (separator < 0) {
                throw new IllegalArgumentException("Illegal option: " + arg);
            }
            options.put(arg.substring(2, separator), arg.substring(separator + 1));
        }
        return options;
    }

    private static String requireOption(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Missing required option --" + name + "=<value>");
        }
        return value;
    }

    private static List<File> splitPaths(String paths) {
        List<File> dirs = new ArrayList<>();
        for (String path : paths.split(",")) {
            String trimmed = path.trim();
            if (!trimmed.isEmpty()) {
                dirs.add(new File(trimmed));
            }
        }
        if (dirs.isEmpty()) {
            throw new IllegalArgumentException("No source directory in --path=" + paths);
        }
        return dirs;
    }
}
