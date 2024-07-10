/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import io.debezium.antlr.AntlrDdlParserListener;
import io.debezium.antlr.ProxyParseTreeListenerUtil;
import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.connector.mysql.antlr.listener.AlterViewParserListener;
import io.debezium.connector.mysql.antlr.listener.CreateAndAlterDatabaseParserListener;
import io.debezium.connector.mysql.antlr.listener.CreateTableParserListener;
import io.debezium.connector.mysql.antlr.listener.CreateUniqueIndexParserListener;
import io.debezium.connector.mysql.antlr.listener.CreateViewParserListener;
import io.debezium.connector.mysql.antlr.listener.DropDatabaseParserListener;
import io.debezium.connector.mysql.antlr.listener.DropTableParserListener;
import io.debezium.connector.mysql.antlr.listener.DropViewParserListener;
import io.debezium.connector.mysql.antlr.listener.RenameTableParserListener;
import io.debezium.connector.mysql.antlr.listener.SetStatementParserListener;
import io.debezium.connector.mysql.antlr.listener.TruncateTableParserListener;
import io.debezium.connector.mysql.antlr.listener.UseStatementParserListener;
import io.debezium.ddl.parser.mysql.generated.MySqlParser;
import io.debezium.ddl.parser.mysql.generated.MySqlParserBaseListener;
import io.debezium.text.ParsingException;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

/**
 * Copied from:
 * https://github.com/apache/flink-cdc/blob/release-3.1.1/flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql/src/main/java/org/apache/flink/cdc/connectors/mysql/source/parser/CustomMySqlAntlrDdlParserListener.java
 *
 * */
public class CustomMySqlAntlrDdlParserListener extends MySqlParserBaseListener
        implements AntlrDdlParserListener {

    /** Collection of listeners for delegation of events. */
    private final List<ParseTreeListener> listeners = new CopyOnWriteArrayList<>();

    /** Flag for skipping phase. */
    private boolean skipNodes;

    /**
     * Count of skipped nodes. Each enter event during skipping phase will increase the counter and
     * each exit event will decrease it. When counter will be decreased to 0, the skipping phase
     * will end.
     */
    private int skippedNodesCount = 0;

    /** Collection of catched exceptions. */
    private final Collection<ParsingException> errors = new ArrayList<>();

    public CustomMySqlAntlrDdlParserListener(
            MySqlAntlrDdlParser parser, LinkedList<SchemaChangeEvent> parsedEvents) {
        // initialize listeners
        listeners.add(new CreateAndAlterDatabaseParserListener(parser));
        listeners.add(new DropDatabaseParserListener(parser));
        listeners.add(new CreateTableParserListener(parser, listeners));
        listeners.add(new CustomAlterTableParserListener(parser, listeners, parsedEvents));
        listeners.add(new DropTableParserListener(parser));
        listeners.add(new RenameTableParserListener(parser));
        listeners.add(new TruncateTableParserListener(parser));
        listeners.add(new CreateViewParserListener(parser, listeners));
        listeners.add(new AlterViewParserListener(parser, listeners));
        listeners.add(new DropViewParserListener(parser));
        listeners.add(new CreateUniqueIndexParserListener(parser));
        listeners.add(new SetStatementParserListener(parser));
        listeners.add(new UseStatementParserListener(parser));
    }

    /**
     * Returns all caught errors during tree walk.
     *
     * @return list of Parsing exceptions
     */
    @Override
    public Collection<ParsingException> getErrors() {
        return errors;
    }

    @Override
    public void enterEveryRule(ParserRuleContext ctx) {
        if (skipNodes) {
            skippedNodesCount++;
        } else {
            ProxyParseTreeListenerUtil.delegateEnterRule(ctx, listeners, errors);
        }
    }

    @Override
    public void exitEveryRule(ParserRuleContext ctx) {
        if (skipNodes) {
            if (skippedNodesCount == 0) {
                // back in the node where skipping started
                skipNodes = false;
            } else {
                // going up in a tree, means decreasing a number of skipped nodes
                skippedNodesCount--;
            }
        } else {
            ProxyParseTreeListenerUtil.delegateExitRule(ctx, listeners, errors);
        }
    }

    @Override
    public void visitErrorNode(ErrorNode node) {
        ProxyParseTreeListenerUtil.visitErrorNode(node, listeners, errors);
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        ProxyParseTreeListenerUtil.visitTerminal(node, listeners, errors);
    }

    @Override
    public void enterRoutineBody(MySqlParser.RoutineBodyContext ctx) {
        // this is a grammar rule for BEGIN ... END part of statements. Skip it.
        skipNodes = true;
    }
}
