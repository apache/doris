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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParser.ErrorIdentContext;
import org.apache.doris.nereids.DorisParser.NonReservedContext;
import org.apache.doris.nereids.DorisParser.QuotedIdentifierContext;
import org.apache.doris.nereids.DorisParserBaseListener;
import org.apache.doris.nereids.errors.QueryParsingErrors;

import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNodeImpl;

import java.util.function.Function;

/**
 * Do some post processor after parse to facilitate subsequent analysis.
 */
public class PostProcessor extends DorisParserBaseListener {
    @Override
    public void exitErrorIdent(ErrorIdentContext ctx) {
        String ident = ctx.getParent().getText();
        throw QueryParsingErrors.unquotedIdentifierError(ident, ctx);
    }

    @Override
    public void exitQuotedIdentifier(QuotedIdentifierContext ctx) {
        replaceTokenByIdentifier(ctx, 1, token -> {
            // Remove the double back ticks in the string.
            token.setText(token.getText().replace("``", "`"));
            return token;
        });
    }

    @Override
    public void exitNonReserved(NonReservedContext ctx) {
        replaceTokenByIdentifier(ctx, 0, i -> i);
    }

    private void replaceTokenByIdentifier(ParserRuleContext ctx, int stripMargins,
            Function<CommonToken, CommonToken> f) {
        ParserRuleContext parent = ctx.getParent();
        parent.removeLastChild();
        Token token = (Token) (ctx.getChild(0).getPayload());
        CommonToken newToken = new CommonToken(
                new org.antlr.v4.runtime.misc.Pair<>(token.getTokenSource(), token.getInputStream()),
                DorisParser.IDENTIFIER,
                token.getChannel(),
                token.getStartIndex() + stripMargins,
                token.getStopIndex() - stripMargins);
        parent.addChild(new TerminalNodeImpl(f.apply(newToken)));
    }
}
