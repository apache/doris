package org.apache.doris.nereids.pattern.generator;

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;

import java.util.Set;
import java.util.TreeSet;

public class LogicalLeafPatternGenerator extends PatternGenerator {

    public LogicalLeafPatternGenerator(OperatorAnalyzer analyzer, ClassDeclaration opType, Set<String> parentClass) {
        super(analyzer, opType, parentClass);
    }

    @Override
    public String generate() {
        String opClassName = opType.name;
        String methodName = getPatternMethodName();

        String patternParam = "<LogicalLeaf<" + opClassName + ">, Plan>";

        generateTypePattern(methodName, opClassName, patternParam, "", false);

        for (EnumFieldPatternInfo info : enumFieldPatternInfos) {
            String predicate = ".when(p -> p.operator." + info.enumInstanceGetter + "() == "
                    + info.enumType + "." + info.enumInstance + ")";
            generateTypePattern(info.patternName, opClassName, patternParam, predicate, false);
        }

        return generatePatterns();
    }

    @Override
    public String genericType() {
        return  "<LogicalLeaf<" + opType.name + ">, Plan>";
    }

    @Override
    public String genericTypeWithChildren() {
        throw new IllegalStateException("Can not get children generic type by LeafPlan");
    }

    @Override
    public Set<String> getImports() {
        Set<String> imports = new TreeSet<>();
        imports.add(opType.getFullQualifiedName());
        imports.add("org.apache.doris.nereids.trees.plans.Plan");
        imports.add("org.apache.doris.nereids.trees.plans.logical.LogicalLeaf");
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
        return 0;
    }
}
