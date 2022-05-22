package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.util.Set;
import java.util.TreeSet;

public class LogicalBinaryPatternGenerator extends PatternGenerator {

    public LogicalBinaryPatternGenerator(OperatorAnalyzer analyzer, ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String generate() {
        String opClassName = opType.name;
        String methodName = getPatternMethodName();

        String generateParamPlan = "<LogicalBinary<" + opClassName + ", Plan, Plan>, Plan>";
        String generateParamChildren = "<LogicalBinary<" + opClassName + ", C1, C2>, Plan>";

        String patterns = "default PatternDescriptor" + generateParamPlan + " " + methodName + "() {\n"
                + "    return new PatternDescriptor" + generateParamPlan + "(\n"
                + "        new TypePattern(" + opClassName + ".class),\n"
                + "        defaultPromise()\n"
                + "    );\n"
                + "}\n"
                + "\n"
                + "default <C1 extends Plan, C2 extends Plan>\n"
                + "PatternDescriptor" + generateParamChildren + "\n"
                + "        " + methodName + "(PatternDescriptor<C1, Plan> leftChildPattern,"
                + " PatternDescriptor<C2, Plan> rightChildPattern) {\n"
                + "    return new PatternDescriptor" + generateParamChildren + "(\n"
                + "        new TypePattern(" + opClassName + ".class, leftChildPattern.pattern,"
                + " rightChildPattern.pattern),\n"
                + "        defaultPromise()\n"
                + "    );\n"
                + "}\n"
                + "\n";

        for (EnumFieldPatternInfo info : enumFieldPatternInfos) {
            patterns +=
                    "default PatternDescriptor" + generateParamPlan + " " + info.patternName + "() {\n"
                    + "    return new PatternDescriptor" + generateParamPlan + "(\n"
                    + "        new TypePattern(" + opClassName + ".class),\n"
                    + "        defaultPromise()\n"
                    + "    ).when(p -> p.op." + info.enumInstanceGetter + "() == "
                            + info.enumType + "." + info.enumInstance + ");\n"
                    + "}\n"
                    + "\n"
                    + "default <C1 extends Plan, C2 extends Plan>\n"
                    + "PatternDescriptor" + generateParamChildren + "\n"
                    + "        " + info.patternName + "(PatternDescriptor<C1, Plan> leftChildPattern,"
                    + " PatternDescriptor<C2, Plan> rightChildPattern) {\n"
                    + "    return new PatternDescriptor" + generateParamChildren + "(\n"
                    + "        new TypePattern(" + opClassName + ".class, leftChildPattern.pattern,"
                    + " rightChildPattern.pattern),\n"
                    + "        defaultPromise()\n"
                    + "    ).when(p -> p.op." + info.enumInstanceGetter + "() == "
                            + info.enumType + "." + info.enumInstance + ");\n"
                    + "}\n\n";
        }

        return patterns;
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.trees.plans.Plan");
        imports.add("org.apache.doris.nereids.trees.plans.logical.LogicalBinary");
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
        return 2;
    }
}
