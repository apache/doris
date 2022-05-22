package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.util.Set;
import java.util.TreeSet;

public class LogicalUnaryPatternGenerator extends PatternGenerator {

    public LogicalUnaryPatternGenerator(OperatorAnalyzer analyzer, ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String generate() {
        String opClassName = opType.name;
        String methodName = getPatternMethodName();
        return "default PatternDescriptor<LogicalUnary<" + opClassName + ", Plan>, Plan> " + methodName + "() {\n"
                + "    return new PatternDescriptor<>(\n"
                + "        new TypePattern(" + opClassName + ".class),\n"
                + "        defaultPromise()\n"
                + "    );\n"
                + "}\n"
                + "\n"
                + "default <C extends Plan>\n"
                + "PatternDescriptor<LogicalUnary<" + opClassName + ", C>, Plan>\n"
                + "        " + methodName + "(PatternDescriptor<C, Plan> childPattern) {\n"
                + "    return new PatternDescriptor<>(\n"
                + "        new TypePattern(" + opClassName + ".class, childPattern.pattern),\n"
                + "        defaultPromise()\n"
                + "    );\n"
                + "}\n";
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.trees.plans.Plan");
        imports.add("org.apache.doris.nereids.trees.plans.logical.LogicalUnary");
        enumFieldPatternInfos.stream()
                .map(info -> info.enumFullName)
                .forEach(imports::add);
        return imports;
    }

    @Override
    public boolean isLogical() {
        return true;
    }

    @Override
    public int childrenNum() {
        return 1;
    }
}
