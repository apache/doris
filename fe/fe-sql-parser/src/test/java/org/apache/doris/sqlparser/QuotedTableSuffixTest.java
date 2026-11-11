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

package org.apache.doris.sqlparser;

import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParserBaseVisitor;
import org.apache.doris.nereids.exceptions.ParseException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class QuotedTableSuffixTest {
    private final DorisSqlParser parser = new DorisSqlParser();

    @Test
    void rejectsUnquotedSuffixAttachedToQuotedTable() {
        for (String table : List.of("`catalog`.`db`.`t`", "catalog.db.`t`", "`t`", "`t``name`", "`表`")) {
            for (String suffix : List.of("$snapshots", "$history", "$partitions", "$custom")) {
                String sql = "SELECT * FROM " + table + suffix;
                Assertions.assertThrows(ParseException.class, () -> parser.parseStatement(sql), sql);
            }
        }
    }

    @Test
    void rejectsAttachedSuffixWithoutBuildingParseTree() {
        DorisParser generated = parser.newParser(parser.newLexer("SELECT * FROM `t`$snapshots"));
        generated.setBuildParseTree(false);
        Assertions.assertThrows(ParseException.class, generated::singleStatement);
    }

    @Test
    void preservesCompleteSystemTableIdentifiers() {
        for (String table : List.of("catalog.db.t$snapshots", "`catalog`.`db`.`t$snapshots`")) {
            DorisParser.TableNameContext relation = parseTable("SELECT * FROM " + table);
            Assertions.assertEquals("catalog.db.t$snapshots", relation.multipartIdentifier().getText());
            Assertions.assertNull(relation.tableAlias().strictIdentifier());
        }
    }

    @Test
    void preservesExplicitAndSeparatedDollarAliases() {
        for (String separator : List.of(" ", "\t", "\n", "/**/", "-- alias\n", " AS ", "AS ")) {
            String sql = "SELECT $snapshots.id FROM `t`" + separator + "$snapshots";
            DorisParser.TableNameContext relation = parseTable(sql);
            Assertions.assertEquals("t", relation.multipartIdentifier().getText());
            Assertions.assertEquals("$snapshots", relation.tableAlias().strictIdentifier().getText());
        }
    }

    @Test
    void preservesQuotedAndOrdinaryAliases() {
        for (String alias : List.of(" `$snapshots`", "ordinary", " `ordinary`")) {
            DorisParser.TableNameContext relation = parseTable("SELECT * FROM `t`" + alias);
            Assertions.assertEquals("t", relation.multipartIdentifier().getText());
            Assertions.assertEquals(alias.trim().replace("`", ""), relation.tableAlias().strictIdentifier().getText());
        }
    }

    private DorisParser.TableNameContext parseTable(String sql) {
        List<DorisParser.TableNameContext> relations = new ArrayList<>();
        new DorisParserBaseVisitor<Void>() {
            @Override
            public Void visitTableName(DorisParser.TableNameContext ctx) {
                relations.add(ctx);
                return super.visitTableName(ctx);
            }
        }.visit(parser.parseStatement(sql));
        Assertions.assertEquals(1, relations.size());
        return relations.get(0);
    }
}
