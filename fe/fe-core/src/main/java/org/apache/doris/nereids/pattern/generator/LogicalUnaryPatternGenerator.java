package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.util.Set;
import java.util.TreeSet;

public class LogicalUnaryPatternGenerator extends PatternGenerator {

    public LogicalUnaryPatternGenerator(OperatorAnalyzer analyzer, ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String genericType() {
        return "<LogicalUnary<" + opType.name + ", Plan>, Plan>";
    }

    @Override
    public String genericTypeWithChildren() {
        return "<LogicalUnary<" + opType.name + ", C1>, Plan>";
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.operators.OperatorType");
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
