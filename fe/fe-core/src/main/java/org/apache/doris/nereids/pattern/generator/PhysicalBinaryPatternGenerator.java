package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.util.Set;
import java.util.TreeSet;

public class PhysicalBinaryPatternGenerator extends PatternGenerator {

    public PhysicalBinaryPatternGenerator(OperatorAnalyzer analyzer, ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String genericType() {
        return "<PhysicalBinary<" + opType.name + ", Plan, Plan>, Plan>";
    }

    @Override
    public String genericTypeWithChildren() {
        return "<PhysicalBinary<" + opType.name + ", C1, C2>, Plan>";
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.operators.OperatorType");
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
