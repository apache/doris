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

package org.apache.doris.nereids.exceptions;

import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.parser.ParserUtils;

import org.antlr.v4.runtime.ParserRuleContext;

import java.util.Optional;

/**
 * sql parsing exception.
 */
public class ParseException extends AnalysisException {
    private final String message;
    private final Origin start;
    private final Optional<String> command;

    public ParseException(String message) {
        this(message, new Origin(0, 0), Optional.empty());
    }

    public ParseException(String message, Origin start, Optional<String> command) {
        super(message, start.line, start.startPosition, Optional.empty());
        this.message = message;
        this.start = start;
        this.command = command;
    }

    public ParseException(String message, ParserRuleContext ctx) {
        this(message, ParserUtils.position(ctx.getStart()), Optional.of(ParserUtils.command(ctx)));
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append(message);
        if (start.line.isPresent() && start.startPosition.isPresent()) {
            int line = start.line.get();
            int startPosition = start.startPosition.get();
            sb.append("(line ").append(line).append(", pos ").append(startPosition).append(")").append("\n");
            if (command.isPresent()) {
                sb.append("\n== SQL ==\n");
                String cmd = command.get();
                String[] splitCmd = cmd.split("\n");
                for (int i = 0; i < line; i++) {
                    sb.append(splitCmd[i]).append("\n");
                }
                for (int i = 0; i < startPosition; i++) {
                    sb.append("-");
                }
                sb.append("^^^\n");
                for (int i = line; i < splitCmd.length; i++) {
                    sb.append(splitCmd[i]).append("\n");
                }
            }
        }
        return sb.toString();
    }
}
