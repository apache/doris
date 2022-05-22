package org.apache.doris.nereids.pattern.generator.javaast;

public class MethodDeclaration extends MemberDeclaration {
    public final TypeTypeOrVoid typeTypeOrVoid;
    public final String identifier;
    public final int paramNum;

    public MethodDeclaration(TypeTypeOrVoid typeTypeOrVoid, String identifier, int paramNum) {
        this.typeTypeOrVoid = typeTypeOrVoid;
        this.identifier = identifier;
        this.paramNum = paramNum;
    }

    @Override
    public String toString() {
        return typeTypeOrVoid.toString() + " " + identifier + "();";
    }
}
