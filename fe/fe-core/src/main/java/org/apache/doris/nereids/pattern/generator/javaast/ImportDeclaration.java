package org.apache.doris.nereids.pattern.generator.javaast;

public class ImportDeclaration implements JavaAstNode {
    public final boolean isStatic;
    public final QualifiedName name;
    public final boolean importAll;

    public ImportDeclaration(boolean isStatic, QualifiedName name, boolean importAll) {
        this.isStatic = isStatic;
        this.name = name;
        this.importAll = importAll;
    }

    @Override
    public String toString() {
        return "import " + (isStatic ? "static " : "") + name + (importAll ? ".*" : "") + ";";
    }
}
