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

import org.apache.doris.nereids.pattern.generator.javaast.ClassDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.ClassOrInterfaceModifier;
import org.apache.doris.nereids.pattern.generator.javaast.ClassOrInterfaceType;
import org.apache.doris.nereids.pattern.generator.javaast.EnumConstant;
import org.apache.doris.nereids.pattern.generator.javaast.EnumDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.FieldDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.IdentifyTypeArgumentsPair;
import org.apache.doris.nereids.pattern.generator.javaast.ImportDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.InterfaceDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.MethodDeclaration;
import org.apache.doris.nereids.pattern.generator.javaast.QualifiedName;
import org.apache.doris.nereids.pattern.generator.javaast.TypeArgument;
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Reads and writes the java ast nodes of the pattern generator.
 *
 * <p>The ast is cached between builds (see {@link PatternGeneratorCache}), which needs a stable,
 * dependency free and versioned encoding. The jdk serialization cannot be used here because the
 * ast nodes hold {@link Optional} fields.
 *
 * <p>Adding a field to an ast node requires a new node tag or a format version bump in
 * {@link PatternGeneratorCache}, otherwise an old cache would be read back incorrectly.
 */
public class JavaAstCodec {
    private static final int NODE_CLASS = 1;
    private static final int NODE_INTERFACE = 2;
    private static final int NODE_ENUM = 3;
    private static final int NODE_CLASS_OR_INTERFACE_TYPE = 4;
    private static final int NODE_IDENTIFY_TYPE_ARGUMENTS_PAIR = 5;
    private static final int NODE_TYPE_ARGUMENTS = 6;
    private static final int NODE_TYPE_ARGUMENT = 7;
    private static final int NODE_TYPE_PARAMETERS = 8;
    private static final int NODE_TYPE_PARAMETER = 9;
    private static final int NODE_TYPE_BOUND = 10;
    private static final int NODE_TYPE_TYPE = 11;
    private static final int NODE_TYPE_TYPE_OR_VOID = 12;
    private static final int NODE_QUALIFIED_NAME = 13;
    private static final int NODE_IMPORT = 14;
    private static final int NODE_MODIFIER = 15;
    private static final int NODE_ENUM_CONSTANT = 16;
    private static final int NODE_FIELD = 17;
    private static final int NODE_VARIABLE_DECLARATORS = 18;
    private static final int NODE_VARIABLE_DECLARATOR = 19;
    private static final int NODE_VARIABLE_DECLARATOR_ID = 20;
    private static final int NODE_METHOD = 21;

    /** read a list of type declarations. */
    public static List<TypeDeclaration> readDeclarations(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<TypeDeclaration> declarations = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            declarations.add(readTypeDeclaration(in));
        }
        return declarations;
    }

    /** write a list of type declarations. */
    public static void writeDeclarations(DataOutputStream out, List<TypeDeclaration> declarations)
            throws IOException {
        out.writeInt(declarations.size());
        for (TypeDeclaration declaration : declarations) {
            writeTypeDeclaration(out, declaration);
        }
    }

    /** read a single type declaration. */
    public static TypeDeclaration readTypeDeclaration(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        Optional<QualifiedName> packageName = readOptional(in, JavaAstCodec::readQualifiedName);
        List<ImportDeclaration> imports = readList(in, JavaAstCodec::readImport);
        ClassOrInterfaceModifier modifiers = readModifier(in);
        String name = in.readUTF();
        List<TypeDeclaration> children = readList(in, JavaAstCodec::readTypeDeclaration);
        switch (tag) {
            case NODE_CLASS:
                return new ClassDeclaration(packageName.orElse(null), imports, modifiers, name,
                        readOptional(in, JavaAstCodec::readTypeParameters).orElse(null),
                        readOptional(in, JavaAstCodec::readTypeType).orElse(null),
                        readList(in, JavaAstCodec::readTypeType),
                        readList(in, JavaAstCodec::readFieldDeclaration),
                        readList(in, JavaAstCodec::readMethodDeclaration),
                        children);
            case NODE_INTERFACE:
                return new InterfaceDeclaration(packageName.orElse(null), imports, modifiers, name,
                        readOptional(in, JavaAstCodec::readTypeParameters).orElse(null),
                        readList(in, JavaAstCodec::readTypeType),
                        children);
            case NODE_ENUM:
                return new EnumDeclaration(packageName.orElse(null), imports, modifiers, name,
                        readList(in, JavaAstCodec::readTypeType),
                        readList(in, JavaAstCodec::readEnumConstant),
                        children);
            default:
                throw new IOException("Unknown type declaration tag " + tag);
        }
    }

    /** write a single type declaration. */
    public static void writeTypeDeclaration(DataOutputStream out, TypeDeclaration declaration) throws IOException {
        if (declaration instanceof ClassDeclaration) {
            out.writeByte(NODE_CLASS);
        } else if (declaration instanceof InterfaceDeclaration) {
            out.writeByte(NODE_INTERFACE);
        } else if (declaration instanceof EnumDeclaration) {
            out.writeByte(NODE_ENUM);
        } else {
            throw new IOException("Unsupported type declaration " + declaration.getClass().getName());
        }
        writeOptional(out, declaration.packageName, JavaAstCodec::writeQualifiedName);
        writeList(out, declaration.imports, JavaAstCodec::writeImport);
        writeModifier(out, declaration.modifiers);
        out.writeUTF(declaration.name);
        writeList(out, declaration.children, JavaAstCodec::writeTypeDeclaration);

        if (declaration instanceof ClassDeclaration) {
            ClassDeclaration classDeclaration = (ClassDeclaration) declaration;
            writeOptional(out, classDeclaration.typeParameters, JavaAstCodec::writeTypeParameters);
            writeOptional(out, classDeclaration.extendsType, JavaAstCodec::writeTypeType);
            writeList(out, classDeclaration.implementTypes, JavaAstCodec::writeTypeType);
            writeList(out, classDeclaration.fieldDeclarations, JavaAstCodec::writeFieldDeclaration);
            writeList(out, classDeclaration.methodDeclarations, JavaAstCodec::writeMethodDeclaration);
        } else if (declaration instanceof InterfaceDeclaration) {
            InterfaceDeclaration interfaceDeclaration = (InterfaceDeclaration) declaration;
            writeOptional(out, interfaceDeclaration.typeParameters, JavaAstCodec::writeTypeParameters);
            writeList(out, interfaceDeclaration.extendsTypes, JavaAstCodec::writeTypeType);
        } else {
            EnumDeclaration enumDeclaration = (EnumDeclaration) declaration;
            writeList(out, enumDeclaration.implementTypes, JavaAstCodec::writeTypeType);
            writeList(out, enumDeclaration.constants, JavaAstCodec::writeEnumConstant);
        }
    }

    private static TypeType readTypeType(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_TYPE);
        return new TypeType(readOptional(in, JavaAstCodec::readClassOrInterfaceType).orElse(null),
                in.readBoolean() ? in.readUTF() : null);
    }

    private static void writeTypeType(DataOutputStream out, TypeType type) throws IOException {
        out.writeByte(NODE_TYPE_TYPE);
        writeOptional(out, type.classOrInterfaceType, JavaAstCodec::writeClassOrInterfaceType);
        writeOptionalString(out, type.primitiveType);
    }

    private static TypeTypeOrVoid readTypeTypeOrVoid(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_TYPE_OR_VOID);
        TypeType typeType = readOptional(in, JavaAstCodec::readTypeType).orElse(null);
        return new TypeTypeOrVoid(typeType, in.readBoolean());
    }

    private static void writeTypeTypeOrVoid(DataOutputStream out, TypeTypeOrVoid typeTypeOrVoid) throws IOException {
        out.writeByte(NODE_TYPE_TYPE_OR_VOID);
        writeOptional(out, typeTypeOrVoid.typeType, JavaAstCodec::writeTypeType);
        out.writeBoolean(typeTypeOrVoid.isVoid);
    }

    private static ClassOrInterfaceType readClassOrInterfaceType(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_CLASS_OR_INTERFACE_TYPE);
        return new ClassOrInterfaceType(readList(in, JavaAstCodec::readIdentifyTypeArgumentsPair));
    }

    private static void writeClassOrInterfaceType(DataOutputStream out, ClassOrInterfaceType type) throws IOException {
        out.writeByte(NODE_CLASS_OR_INTERFACE_TYPE);
        writeList(out, type.identifyTypeArguments, JavaAstCodec::writeIdentifyTypeArgumentsPair);
    }

    private static IdentifyTypeArgumentsPair readIdentifyTypeArgumentsPair(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_IDENTIFY_TYPE_ARGUMENTS_PAIR);
        String identifier = in.readUTF();
        TypeArguments typeArguments = readOptional(in, JavaAstCodec::readTypeArguments).orElse(null);
        return new IdentifyTypeArgumentsPair(identifier, typeArguments);
    }

    private static void writeIdentifyTypeArgumentsPair(DataOutputStream out, IdentifyTypeArgumentsPair pair)
            throws IOException {
        out.writeByte(NODE_IDENTIFY_TYPE_ARGUMENTS_PAIR);
        out.writeUTF(pair.identifier);
        writeOptional(out, pair.typeArguments, JavaAstCodec::writeTypeArguments);
    }

    private static TypeArguments readTypeArguments(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_ARGUMENTS);
        return new TypeArguments(readList(in, JavaAstCodec::readTypeArgument));
    }

    private static void writeTypeArguments(DataOutputStream out, TypeArguments typeArguments) throws IOException {
        out.writeByte(NODE_TYPE_ARGUMENTS);
        writeList(out, typeArguments.typeArguments, JavaAstCodec::writeTypeArgument);
    }

    private static TypeArgument readTypeArgument(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_ARGUMENT);
        TypeArgument.ArgType argType = TypeArgument.ArgType.values()[in.readUnsignedByte()];
        TypeType typeType = readOptional(in, JavaAstCodec::readTypeType).orElse(null);
        return new TypeArgument(argType, typeType);
    }

    private static void writeTypeArgument(DataOutputStream out, TypeArgument typeArgument) throws IOException {
        out.writeByte(NODE_TYPE_ARGUMENT);
        out.writeByte(typeArgument.getArgType().ordinal());
        writeOptional(out, typeArgument.getTypeType(), JavaAstCodec::writeTypeType);
    }

    private static TypeParameters readTypeParameters(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_PARAMETERS);
        return new TypeParameters(readList(in, JavaAstCodec::readTypeParameter));
    }

    private static void writeTypeParameters(DataOutputStream out, TypeParameters typeParameters) throws IOException {
        out.writeByte(NODE_TYPE_PARAMETERS);
        writeList(out, typeParameters.typeParameters, JavaAstCodec::writeTypeParameter);
    }

    private static TypeParameter readTypeParameter(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_PARAMETER);
        String identifier = in.readUTF();
        TypeBound typeBound = readOptional(in, JavaAstCodec::readTypeBound).orElse(null);
        return new TypeParameter(identifier, typeBound);
    }

    private static void writeTypeParameter(DataOutputStream out, TypeParameter typeParameter) throws IOException {
        out.writeByte(NODE_TYPE_PARAMETER);
        out.writeUTF(typeParameter.identifier);
        writeOptional(out, typeParameter.typeBound, JavaAstCodec::writeTypeBound);
    }

    private static TypeBound readTypeBound(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_TYPE_BOUND);
        return new TypeBound(readList(in, JavaAstCodec::readTypeType));
    }

    private static void writeTypeBound(DataOutputStream out, TypeBound typeBound) throws IOException {
        out.writeByte(NODE_TYPE_BOUND);
        writeList(out, typeBound.types, JavaAstCodec::writeTypeType);
    }

    private static QualifiedName readQualifiedName(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_QUALIFIED_NAME);
        return new QualifiedName(readStringList(in));
    }

    private static void writeQualifiedName(DataOutputStream out, QualifiedName qualifiedName) throws IOException {
        out.writeByte(NODE_QUALIFIED_NAME);
        writeStringList(out, qualifiedName.identifiers);
    }

    private static ImportDeclaration readImport(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_IMPORT);
        boolean isStatic = in.readBoolean();
        QualifiedName name = readQualifiedName(in);
        return new ImportDeclaration(isStatic, name, in.readBoolean());
    }

    private static void writeImport(DataOutputStream out, ImportDeclaration importDeclaration) throws IOException {
        out.writeByte(NODE_IMPORT);
        out.writeBoolean(importDeclaration.isStatic);
        writeQualifiedName(out, importDeclaration.name);
        out.writeBoolean(importDeclaration.importAll);
    }

    private static ClassOrInterfaceModifier readModifier(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_MODIFIER);
        return new ClassOrInterfaceModifier(in.readInt());
    }

    private static void writeModifier(DataOutputStream out, ClassOrInterfaceModifier modifier) throws IOException {
        out.writeByte(NODE_MODIFIER);
        out.writeInt(modifier.mod);
    }

    private static EnumConstant readEnumConstant(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_ENUM_CONSTANT);
        return new EnumConstant(in.readUTF());
    }

    private static void writeEnumConstant(DataOutputStream out, EnumConstant enumConstant) throws IOException {
        out.writeByte(NODE_ENUM_CONSTANT);
        out.writeUTF(enumConstant.identifier);
    }

    private static FieldDeclaration readFieldDeclaration(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_FIELD);
        TypeType type = readTypeType(in);
        VariableDeclarators declarators = readVariableDeclarators(in);
        return new FieldDeclaration(type, declarators);
    }

    private static void writeFieldDeclaration(DataOutputStream out, FieldDeclaration fieldDeclaration)
            throws IOException {
        out.writeByte(NODE_FIELD);
        writeTypeType(out, fieldDeclaration.type);
        writeVariableDeclarators(out, fieldDeclaration.variableDeclarators);
    }

    private static VariableDeclarators readVariableDeclarators(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_VARIABLE_DECLARATORS);
        return new VariableDeclarators(readList(in, JavaAstCodec::readVariableDeclarator));
    }

    private static void writeVariableDeclarators(DataOutputStream out, VariableDeclarators declarators)
            throws IOException {
        out.writeByte(NODE_VARIABLE_DECLARATORS);
        writeList(out, declarators.variableDeclarators, JavaAstCodec::writeVariableDeclarator);
    }

    private static VariableDeclarator readVariableDeclarator(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_VARIABLE_DECLARATOR);
        return new VariableDeclarator(readVariableDeclaratorId(in));
    }

    private static void writeVariableDeclarator(DataOutputStream out, VariableDeclarator declarator)
            throws IOException {
        out.writeByte(NODE_VARIABLE_DECLARATOR);
        writeVariableDeclaratorId(out, declarator.variableDeclaratorId);
    }

    private static VariableDeclaratorId readVariableDeclaratorId(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_VARIABLE_DECLARATOR_ID);
        String identifier = in.readUTF();
        return new VariableDeclaratorId(identifier, in.readInt());
    }

    private static void writeVariableDeclaratorId(DataOutputStream out, VariableDeclaratorId declaratorId)
            throws IOException {
        out.writeByte(NODE_VARIABLE_DECLARATOR_ID);
        out.writeUTF(declaratorId.identifier);
        out.writeInt(declaratorId.arrayDimension);
    }

    private static MethodDeclaration readMethodDeclaration(DataInputStream in) throws IOException {
        int tag = in.readUnsignedByte();
        checkTag(tag, NODE_METHOD);
        TypeTypeOrVoid typeTypeOrVoid = readTypeTypeOrVoid(in);
        String identifier = in.readUTF();
        return new MethodDeclaration(typeTypeOrVoid, identifier, in.readInt());
    }

    private static void writeMethodDeclaration(DataOutputStream out, MethodDeclaration methodDeclaration)
            throws IOException {
        out.writeByte(NODE_METHOD);
        writeTypeTypeOrVoid(out, methodDeclaration.typeTypeOrVoid);
        out.writeUTF(methodDeclaration.identifier);
        out.writeInt(methodDeclaration.paramNum);
    }

    private static <T> List<T> readList(DataInputStream in, ElementReader<T> reader) throws IOException {
        int size = in.readInt();
        List<T> elements = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            elements.add(reader.read(in));
        }
        return elements;
    }

    private static <T> void writeList(DataOutputStream out, List<T> elements, ElementWriter<T> writer)
            throws IOException {
        out.writeInt(elements.size());
        for (T element : elements) {
            writer.write(out, element);
        }
    }

    private static <T> Optional<T> readOptional(DataInputStream in, ElementReader<T> reader) throws IOException {
        return in.readBoolean() ? Optional.of(reader.read(in)) : Optional.empty();
    }

    private static <T> void writeOptional(DataOutputStream out, Optional<T> value, ElementWriter<T> writer)
            throws IOException {
        out.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            writer.write(out, value.get());
        }
    }

    private static void writeOptionalString(DataOutputStream out, Optional<String> value) throws IOException {
        out.writeBoolean(value.isPresent());
        if (value.isPresent()) {
            out.writeUTF(value.get());
        }
    }

    private static List<String> readStringList(DataInputStream in) throws IOException {
        int size = in.readInt();
        List<String> elements = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            elements.add(in.readUTF());
        }
        return elements;
    }

    private static void writeStringList(DataOutputStream out, List<String> elements) throws IOException {
        out.writeInt(elements.size());
        for (String element : elements) {
            out.writeUTF(element);
        }
    }

    private static void checkTag(int actual, int expected) throws IOException {
        if (actual != expected) {
            throw new IOException("Corrupted pattern generator cache, expected tag " + expected + " but got "
                    + actual);
        }
    }

    /** reads one element. */
    private interface ElementReader<T> {
        T read(DataInputStream in) throws IOException;
    }

    /** writes one element. */
    private interface ElementWriter<T> {
        void write(DataOutputStream out, T element) throws IOException;
    }
}
