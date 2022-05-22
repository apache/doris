package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.JavaLexer;
import org.apache.doris.nereids.JavaParser;
import org.apache.doris.nereids.pattern.generator.JavaAstBuilder;
import org.apache.doris.nereids.pattern.generator.OperatorAnalyzer;
import org.apache.doris.nereids.pattern.generator.javaast.TypeDeclaration;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic.Kind;
import javax.tools.StandardLocation;

@SupportedAnnotationTypes("org.apache.doris.nereids.pattern.Describable")
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class DescribableProcessor extends AbstractProcessor {
    private List<File> operatorPaths;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.operatorPaths = Arrays.stream(processingEnv.getOptions().get("operatorPath").split(","))
                .map(path -> path.trim())
                .filter(path -> !path.isEmpty())
                .collect(Collectors.toSet())
                .stream()
                .map(path -> new File(path))
                .collect(Collectors.toList());
        System.out.println("Init DescribableProcessor");
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (annotations.isEmpty()) {
            return false;
        }
        try {
            List<File> operatorFiles = findJavaFiles(operatorPaths);
            OperatorAnalyzer operatorAnalyzer = new OperatorAnalyzer();
            for (File file : operatorFiles) {
                List<TypeDeclaration> asts = parseJavaFile(file);
                operatorAnalyzer.addAsts(asts);
            }
            String generatePatternCode = operatorAnalyzer.generatePatterns();
            File generatePatternFile = new File(processingEnv.getFiler()
                    .getResource(StandardLocation.SOURCE_OUTPUT, "org.apache.doris.nereids.pattern",
                            "GeneratedPatterns.java").toUri());
            if (generatePatternFile.exists()) {
                generatePatternFile.delete();
            }
            if (!generatePatternFile.getParentFile().exists()) {
                generatePatternFile.getParentFile().mkdirs();
            }

            // bypass create file for processingEnv.getFiler(), compile GeneratePatterns in next compile time
            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(generatePatternFile))) {
                bufferedWriter.write(generatePatternCode);
            }
        } catch (Throwable t) {
            String exceptionMsg = Throwables.getStackTraceAsString(t);
            processingEnv.getMessager().printMessage(Kind.ERROR, "Analyze and generate patterns failed:\n" + exceptionMsg);
        }
        return false;
    }

    private List<File> findJavaFiles(List<File> dirs) {
        List<File> files = new ArrayList<>();
        for (File dir : dirs) {
            files.addAll(FileUtils.listFiles(dir, new String[] {"java"}, true));
        }
        return files;
    }

    private List<TypeDeclaration> parseJavaFile(File javaFile) throws IOException {
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

        // parser.addParseListener(PostProcessor)
        // parser.removeErrorListeners()
        // parser.addErrorListener(ParseErrorListener)

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
}
