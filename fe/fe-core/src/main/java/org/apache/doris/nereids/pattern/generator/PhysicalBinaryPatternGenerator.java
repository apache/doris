package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.util.Set;
import java.util.TreeSet;

public class PhysicalBinaryPatternGenerator extends PatternGenerator {

    public PhysicalBinaryPatternGenerator(OperatorAnalyzer analyzer, ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String generate() {
        String opClassName = opType.name;
        String methodName = getPatternMethodName();
        return "default PatternDescriptor<PhysicalBinary<" + opClassName + ", Plan, Plan>, Plan> " + methodName + "() {\n"
                + "    return new PatternDescriptor<>(\n"
                + "        new TypePattern(" + opClassName + ".class),\n"
                + "        defaultPromise()\n"
                + "    );\n"
                + "}\n"
                + "\n"
                + "default <C1 extends Plan, C2 extends Plan>\n"
                + "PatternDescriptor<PhysicalBinary<" + opClassName + ", C1, C2>, Plan>\n"
                + "        " + methodName + "(PatternDescriptor<C1, Plan> leftChildPattern,"
                + " PatternDescriptor<C2, Plan> rightChildPattern) {\n"
                + "    return new PatternDescriptor<>(\n"
                + "        new TypePattern(" + opClassName + ".class, leftChildPattern.pattern,"
                + " rightChildPattern.pattern),\n"
                + "        defaultPromise()\n"
                + "    );\n"
                + "}\n";
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.trees.plans.Plan");
        imports.add("org.apache.doris.nereids.trees.plans.physical.PhysicalBinary");
        enumFieldPatternInfos.stream()
                .map(info -> info.enumFullName)
                .forEach(imports::add);
        return imports;
    }

    @Override
    public boolean isLogical() {
        return false;
    }

    @Override
    public int childrenNum() {
        return 2;
    }
}
