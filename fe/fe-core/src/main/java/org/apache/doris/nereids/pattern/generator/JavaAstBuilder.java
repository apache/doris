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

import org.apache.doris.nereids.JavaParser.ClassBodyDeclarationContext;
import org.apache.doris.nereids.JavaParser.ClassDeclarationContext;
import org.apache.doris.nereids.JavaParser.ClassOrInterfaceModifierContext;
import org.apache.doris.nereids.JavaParser.ClassOrInterfaceTypeContext;
import org.apache.doris.nereids.JavaParser.EnumConstantContext;
import org.apache.doris.nereids.JavaParser.EnumDeclarationContext;
import org.apache.doris.nereids.JavaParser.FieldDeclarationContext;
import org.apache.doris.nereids.JavaParser.FormalParameterListContext;
import org.apache.doris.nereids.JavaParser.IdentifierAndTypeArgumentsContext;
import org.apache.doris.nereids.JavaParser.IdentifierContext;
import org.apache.doris.nereids.JavaParser.ImportDeclarationContext;
import org.apache.doris.nereids.JavaParser.InterfaceDeclarationContext;
import org.apache.doris.nereids.JavaParser.MemberDeclarationContext;
import org.apache.doris.nereids.JavaParser.MethodDeclarationContext;
import org.apache.doris.nereids.JavaParser.PackageDeclarationContext;
import org.apache.doris.nereids.JavaParser.PrimitiveTypeContext;
import org.apache.doris.nereids.JavaParser.QualifiedNameContext;
import org.apache.doris.nereids.JavaParser.TypeArgumentContext;
import org.apache.doris.nereids.JavaParser.TypeArgumentsContext;
import org.apache.doris.nereids.JavaParser.TypeBoundContext;
import org.apache.doris.nereids.JavaParser.TypeDeclarationContext;
import org.apache.doris.nereids.JavaParser.TypeListContext;
import org.apache.doris.nereids.JavaParser.TypeParameterContext;
import org.apache.doris.nereids.JavaParser.TypeParametersContext;
import org.apache.doris.nereids.JavaParser.TypeTypeContext;
import org.apache.doris.nereids.JavaParser.TypeTypeOrVoidContext;
import org.apache.doris.nereids.JavaParser.VariableDeclaratorContext;
import org.apache.doris.nereids.JavaParser.VariableDeclaratorIdContext;
import org.apache.doris.nereids.JavaParser.VariableDeclaratorsContext;
import org.apache.doris.nereids.JavaParserBaseVisitor;
import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.ClassOrInterfaceModifier;
import org.apache.doris.nereids.pattern.generator.javaast.ClassOrInterfaceType;
import org.apache.doris.nereids.pattern.generator.javaast.EnumConstant;
import org.apache.doris.nereids.pattern.generator.javaast.EnumDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.FieldDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.IdentifyTypeArgumentsPair;
import org.apache.doris.nereids.pattern.generator.javaast.ImportDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.InterfaceDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.JavaAstNode;
import org.apache.doris.nereids.pattern.generator.javaast.MethodDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.QualifiedName;
import org.apache.doris.nereids.pattern.generator.javaast.TypeArgument;
import org.apache.doris.nereids.pattern.generator.javaast.TypeArgument.ArgType;
import org.apache.doris.nereids.pattern.generator.javaast.TypeArguments;
import org.apache.doris.nereids.pattern.generator.javaast.TypeBound;
import org.apache.doris.nereids.pattern.generator.javaast.TypeDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.TypeParameter;
import org.apache.doris.nereids.pattern.generator.javaast.TypeParameters;
import org.apache.doris.nereids.pattern.generator.javaast.TypeType;
import org.apache.doris.nereids.pattern.generator.javaast.TypeTypeOrVoid;
import org.apache.doris.nereids.pattern.generator.javaast.VariableDeclarator;
import org.apache.doris.nereids.pattern.generator.javaast.VariableDeclaratorId;
import org.apache.doris.nereids.pattern.generator.javaast.VariableDeclarators;

import org.antlr.v4.runtime.ParserRuleContext;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

/**
 * Used to build a copy from antlr ast.
 */
public class JavaAstBuilder extends JavaParserBaseVisitor<JavaAstNode> {
    Optional<QualifiedName> packageName = Optional.empty();
    List<ImportDeclaration> importDeclarations = new ArrayList<>();
    List<TypeDeclaration> rootTypeDeclarations = new ArrayList<>();
    Stack<List<TypeDeclaration>> childrenStack = new Stack<>();

    public List<TypeDeclaration> build(ParserRuleContext tree) {
        visit(tree);
        return rootTypeDeclarations;
    }

    @Override
    public JavaAstNode visitPackageDeclaration(PackageDeclarationContext ctx) {
        packageName = Optional.of(visitQualifiedName(ctx.qualifiedName()));
        return null;
    }

    @Override
    public ImportDeclaration visitImportDeclaration(ImportDeclarationContext ctx) {
        ImportDeclaration importDeclaration = new ImportDeclaration(ctx.STATIC() != null,
                visitQualifiedName(ctx.qualifiedName()), ctx.importStar() != null);
        importDeclarations.add(importDeclaration);
        return importDeclaration;
    }

    @Override
    public JavaAstNode visitTypeDeclaration(TypeDeclarationContext ctx) {
        ClassOrInterfaceModifier modifier = mergeModifiers(ctx.classOrInterfaceModifier());
        if (ctx.classDeclaration() != null) {
            return visitClassDeclaration(ctx.classDeclaration(), modifier);
        } else if (ctx.interfaceDeclaration() != null) {
            return visitInterfaceDeclaration(ctx.interfaceDeclaration(), modifier);
        } else if (ctx.enumDeclaration() != null) {
            return visitEnumDeclaration(ctx.enumDeclaration(), modifier);
        } else {
            return null;
        }
    }

    /** create enum declaration. */
    public EnumDeclaration visitEnumDeclaration(EnumDeclarationContext ctx, ClassOrInterfaceModifier modifier) {
        List<TypeType> implementTypes = new ArrayList<>();
        if (ctx.IMPLEMENTS() != null) {
            implementTypes = getTypes(ctx.typeList());
        }

        List<EnumConstant> enumConstants = new ArrayList<>();
        if (ctx.enumConstants() != null) {
            enumConstants = visit(EnumConstant.class, ctx.enumConstants().enumConstant());
        }

        childrenStack.add(new ArrayList<>());
        if (ctx.enumBodyDeclarations() != null) {
            // find inner class
            ctx.enumBodyDeclarations().accept(this);
        }

        String enumName = getText(ctx.identifier());
        EnumDeclaration enumDeclaration = new EnumDeclaration(packageName.orElse(null),
                importDeclarations, modifier, enumName, implementTypes, enumConstants, childrenStack.pop());
        addTypeDeclaration(enumDeclaration);

        return enumDeclaration;
    }

    @Override
    public EnumConstant visitEnumConstant(EnumConstantContext ctx) {
        return new EnumConstant(getText(ctx.identifier()));
    }

    /** create interface declaration. */
    public InterfaceDeclaration visitInterfaceDeclaration(
            InterfaceDeclarationContext ctx, ClassOrInterfaceModifier modifier) {
        TypeParameters typeParameters = null;
        if (ctx.typeParameters() != null) {
            typeParameters = visitTypeParameters(ctx.typeParameters());
        }
        List<TypeType> extendsTypes = new ArrayList<>();
        if (ctx.EXTENDS() != null) {
            extendsTypes = getTypes(ctx.typeList().get(0));
        }

        childrenStack.add(new ArrayList<>());
        // find inner class
        ctx.interfaceBody().accept(this);

        String interfaceName = getText(ctx.identifier());
        InterfaceDeclaration interfaceDeclaration = new InterfaceDeclaration(packageName.orElse(null),
                importDeclarations, modifier, interfaceName, typeParameters, extendsTypes, childrenStack.pop());
        addTypeDeclaration(interfaceDeclaration);

        return interfaceDeclaration;
    }

    /** create class declaration. */
    public ClassDeclaration visitClassDeclaration(ClassDeclarationContext ctx, ClassOrInterfaceModifier modifier) {
        TypeParameters typeParameters = null;
        if (ctx.typeParameters() != null) {
            typeParameters = visitTypeParameters(ctx.typeParameters());
        }
        TypeType extendsType = null;
        if (ctx.EXTENDS() != null) {
            extendsType = visitTypeType(ctx.typeType());
        }
        List<TypeType> implementTypes = new ArrayList<>();
        if (ctx.IMPLEMENTS() != null) {
            implementTypes = getTypes(ctx.typeList().get(0));
        }

        childrenStack.add(new ArrayList<>());
        List<FieldDeclaration> fieldDeclarations = new ArrayList<>();
        List<MethodDeclaration> methodDeclarations = new ArrayList<>();
        for (ClassBodyDeclarationContext classBodyCtx : ctx.classBody().classBodyDeclaration()) {
            MemberDeclarationContext memberCtx = classBodyCtx.memberDeclaration();
            if (memberCtx != null) {
                if (memberCtx.fieldDeclaration() != null) {
                    fieldDeclarations.add(visitFieldDeclaration(memberCtx.fieldDeclaration()));
                    continue;
                } else if (memberCtx.methodDeclaration() != null) {
                    methodDeclarations.add(visitMethodDeclaration(memberCtx.methodDeclaration()));
                }
                // find inner class
                memberCtx.accept(this);
            }
        }

        String className = getText(ctx.identifier());
        ClassDeclaration classDeclaration = new ClassDeclaration(
                packageName.orElse(null), importDeclarations,
                modifier, className, typeParameters, extendsType, implementTypes,
                fieldDeclarations, methodDeclarations, childrenStack.pop());
        addTypeDeclaration(classDeclaration);
        return classDeclaration;
    }

    @Override
    public ClassOrInterfaceModifier visitClassOrInterfaceModifier(ClassOrInterfaceModifierContext ctx) {
        int mod = 0;
        if (ctx.PUBLIC() != null) {
            mod |= Modifier.PUBLIC;
        } else if (ctx.PROTECTED() != null) {
            mod |= Modifier.PROTECTED;
        } else if (ctx.PRIVATE() != null) {
            mod |= Modifier.PRIVATE;
        } else if (ctx.STATIC() != null) {
            mod |= Modifier.STATIC;
        } else if (ctx.ABSTRACT() != null) {
            mod |= Modifier.ABSTRACT;
        } else if (ctx.ABSTRACT() != null) {
            mod |= Modifier.FINAL;
        }
        return new ClassOrInterfaceModifier(mod);
    }

    @Override
    public FieldDeclaration visitFieldDeclaration(FieldDeclarationContext ctx) {
        TypeType typeType = visitTypeType(ctx.typeType());
        VariableDeclarators variableDeclarators = visitVariableDeclarators(ctx.variableDeclarators());
        return new FieldDeclaration(typeType, variableDeclarators);
    }

    @Override
    public MethodDeclaration visitMethodDeclaration(MethodDeclarationContext ctx) {
        TypeTypeOrVoid typeTypeOrVoid = visitTypeTypeOrVoid(ctx.typeTypeOrVoid());
        String identifier = getText(ctx.identifier());
        int paramNum = 0;
        FormalParameterListContext paramListCtx = ctx.formalParameters().formalParameterList();
        if (paramListCtx != null && paramListCtx.formalParameter() != null) {
            paramNum = paramListCtx.formalParameter().size() + 1; // + lastFormalParameter
        }
        return new MethodDeclaration(typeTypeOrVoid, identifier, paramNum);
    }

    @Override
    public TypeTypeOrVoid visitTypeTypeOrVoid(TypeTypeOrVoidContext ctx) {
        TypeType typeType = null;
        if (ctx.typeType() != null) {
            typeType = visitTypeType(ctx.typeType());
        }
        return new TypeTypeOrVoid(typeType, ctx.VOID() != null);
    }

    @Override
    public VariableDeclarators visitVariableDeclarators(VariableDeclaratorsContext ctx) {
        List<VariableDeclarator> vars = visit(VariableDeclarator.class, ctx.variableDeclarator());
        return new VariableDeclarators(vars);
    }

    @Override
    public VariableDeclarator visitVariableDeclarator(VariableDeclaratorContext ctx) {
        VariableDeclaratorId id = visitVariableDeclaratorId(ctx.variableDeclaratorId());
        return new VariableDeclarator(id);
    }

    @Override
    public VariableDeclaratorId visitVariableDeclaratorId(VariableDeclaratorIdContext ctx) {
        String text = getText(ctx.identifier());
        int arrayDimension = ctx.arrayDeclarator().size();
        return new VariableDeclaratorId(text, arrayDimension);
    }

    @Override
    public ClassOrInterfaceType visitClassOrInterfaceType(ClassOrInterfaceTypeContext ctx) {
        List<IdentifyTypeArgumentsPair> pairs = new ArrayList<>();
        for (IdentifierAndTypeArgumentsContext identifierAndTypeArgument : ctx.identifierAndTypeArguments()) {
            String identifier = getText(identifierAndTypeArgument.identifier());
            TypeArguments typeArguments = null;
            if (identifierAndTypeArgument.typeArguments() != null) {
                typeArguments = visitTypeArguments(identifierAndTypeArgument.typeArguments());
            }
            pairs.add(new IdentifyTypeArgumentsPair(identifier, typeArguments));
        }
        return new ClassOrInterfaceType(pairs);
    }

    @Override
    public TypeParameters visitTypeParameters(TypeParametersContext ctx) {
        List<TypeParameter> typeParameters = new ArrayList<>();
        for (TypeParameterContext typeParameterContext : ctx.typeParameter()) {
            typeParameters.add(visitTypeParameter(typeParameterContext));
        }
        return new TypeParameters(typeParameters);
    }

    @Override
    public TypeParameter visitTypeParameter(TypeParameterContext ctx) {
        String identifier = getText(ctx.identifier());
        TypeBound typeBound = null;
        if (ctx.typeBound() != null) {
            typeBound = visitTypeBound(ctx.typeBound());
        }
        return new TypeParameter(identifier, typeBound);
    }

    @Override
    public TypeBound visitTypeBound(TypeBoundContext ctx) {
        List<TypeType> typeParameters = new ArrayList<>();
        for (TypeTypeContext typeTypeContext : ctx.typeType()) {
            typeParameters.add(visitTypeType(typeTypeContext));
        }
        return new TypeBound(typeParameters);
    }

    @Override
    public TypeArguments visitTypeArguments(TypeArgumentsContext ctx) {
        List<TypeArgument> typeArguments = new ArrayList<>();
        for (TypeArgumentContext typeArgumentContext : ctx.typeArgument()) {
            typeArguments.add(visitTypeArgument(typeArgumentContext));
        }
        return new TypeArguments(typeArguments);
    }

    @Override
    public TypeArgument visitTypeArgument(TypeArgumentContext ctx) {
        TypeType typeType = null;
        ArgType argType = ArgType.UNKNOWN;
        if (ctx.typeType() != null) {
            typeType = visitTypeType(ctx.typeType());
            if (ctx.EXTENDS() != null) {
                argType = ArgType.EXTENDS;
            } else if (ctx.SUPER() != null) {
                argType = ArgType.SUPER;
            } else {
                argType = ArgType.NORMAL;
            }
        }
        return new TypeArgument(argType, typeType);
    }

    @Override
    public TypeType visitTypeType(TypeTypeContext ctx) {
        ClassOrInterfaceType classOrInterfaceType = null;
        if (ctx.classOrInterfaceType() != null) {
            classOrInterfaceType = visitClassOrInterfaceType(ctx.classOrInterfaceType());
        }
        String primitiveType = null;
        if (ctx.primitiveType() != null) {
            primitiveType = getText(ctx.primitiveType());
        }
        return new TypeType(classOrInterfaceType, primitiveType);
    }

    @Override
    public QualifiedName visitQualifiedName(QualifiedNameContext ctx) {
        List<String> identifiers = new ArrayList<>();
        for (IdentifierContext identifierContext : ctx.identifier()) {
            identifiers.add(getText(identifierContext));
        }
        return new QualifiedName(identifiers);
    }

    /** merge modifiers, e.g public + static + final. */
    public ClassOrInterfaceModifier mergeModifiers(List<ClassOrInterfaceModifierContext> contexts) {
        int mod = 0;
        for (ClassOrInterfaceModifierContext context : contexts) {
            ClassOrInterfaceModifier modifier = visitClassOrInterfaceModifier(context);
            mod |= modifier.mod;
        }
        return new ClassOrInterfaceModifier(mod);
    }

    /** create a type list. */
    public List<TypeType> getTypes(TypeListContext typeListContext) {
        List<TypeType> types = new ArrayList<>();
        for (TypeTypeContext typeTypeContext : typeListContext.typeType()) {
            types.add(visitTypeType(typeTypeContext));
        }
        return types;
    }

    /** create a List by class and contexts. */
    public <T extends JavaAstNode, C extends ParserRuleContext> List<T> visit(Class<T> clazz, List<C> contexts) {
        List<T> list = new ArrayList<>();
        for (C ctx : contexts) {
            list.add((T) ctx.accept(this));
        }
        return list;
    }

    public String getText(IdentifierContext ctx) {
        return ctx.getText();
    }

    public String getText(PrimitiveTypeContext ctx) {
        return ctx.getText();
    }

    private void addTypeDeclaration(TypeDeclaration typeDeclaration) {
        if (!childrenStack.isEmpty()) {
            childrenStack.peek().add(typeDeclaration);
        } else {
            rootTypeDeclarations.add(typeDeclaration);
        }
    }

    /** get full qualified name, e.g. OuterClassName.InnerClassName. */
    public static String getFullQualifiedName(Stack<String> outerClassStack,
            Optional<QualifiedName> packageName, String name) {
        if (!outerClassStack.isEmpty()) {
            return outerClassStack.peek() + "." + name;
        }
        return TypeDeclaration.getFullQualifiedName(packageName, name);
    }

    class OuterClass {
        public final Optional<QualifiedName> packageName;
        public final String name;

        public OuterClass(Optional<QualifiedName> packageName, String name) {
            this.packageName = packageName;
            this.name = name;
        }
    }
}
