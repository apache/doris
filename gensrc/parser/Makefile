# Modifications copyright (C) 2017, Baidu.com, Inc.
# Copyright 2017 The Apache Software Foundation

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This file compile sql_parser.y and sql_scanner.flex
#
#
ifndef JAVA_HOME
$(error "JAVA_HOME is not set")
endif

BUILD_DIR = ${CURDIR}/../build/

OUTPUT_DIR = ${BUILD_DIR}/java/com/baidu/palo/analysis
JAVA = java
PARSER_ARGS = -jar ${BUILD_DIR}/../../thirdparty/installed/lib/jar/java-cup-0.11a.jar \
              -destdir ${OUTPUT_DIR} \
              -parser SqlParser -symbols SqlParserSymbols -expect 20
FLEX_ARGS = -jar ${BUILD_DIR}/../../thirdparty/installed/lib/jar/jflex-1.4.3.jar \
              -d ${OUTPUT_DIR} -q

all: parser lexer
.PHONY: all

${OUTPUT_DIR}:
	mkdir -p $@

# Parser
PARSER_OUTPUT = ${OUTPUT_DIR}/SqlParser.java \
                ${OUTPUT_DIR}/SqlParserSymbols.java

${PARSER_OUTPUT}: sql_parser.y | ${OUTPUT_DIR}
	${JAVA} ${PARSER_ARGS} $<
parser: ${PARSER_OUTPUT}
.PHONY: parser

# Flex
FLEX_OUTPUT = ${OUTPUT_DIR}/SqlScanner.java
${FLEX_OUTPUT}: sql_scanner.flex | ${OUTPUT_DIR}
	${JAVA} ${FLEX_ARGS} $<
lexer: ${FLEX_OUTPUT}
.PHONY: lexer
