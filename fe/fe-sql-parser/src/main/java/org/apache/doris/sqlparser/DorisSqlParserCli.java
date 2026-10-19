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
import org.apache.doris.nereids.exceptions.ParseException;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.Trees;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Command-line entry point for fe-sql-parser. Reads a SQL string from
 * argument, file, or stdin and prints the ANTLR parse tree (CST).
 *
 * Exit codes: 0 success, 1 parse error, 2 usage or I/O error.
 */
public final class DorisSqlParserCli {

    private DorisSqlParserCli() {
    }

    public static void main(String[] args) {
        Options opts;
        try {
            opts = parseArgs(args);
        } catch (UsageException e) {
            System.err.println("doris-sql-parse: " + e.getMessage());
            printUsage(System.err);
            System.exit(2);
            return;
        }
        if (opts.help) {
            printUsage(System.out);
            return;
        }

        String sql;
        try {
            sql = readSql(opts);
        } catch (IOException e) {
            System.err.println("doris-sql-parse: failed to read SQL: " + e.getMessage());
            System.exit(2);
            return;
        }
        if (sql == null || sql.trim().isEmpty()) {
            System.err.println("doris-sql-parse: empty SQL input");
            System.exit(2);
            return;
        }

        DorisSqlParser parser = new DorisSqlParser(opts.noBackslashEscapes, opts.ansiSql);
        try {
            ParserRuleContext tree;
            switch (opts.mode) {
                case MULTI:
                    tree = parser.parseStatements(sql);
                    break;
                case EXPRESSION:
                    tree = parser.parseExpression(sql);
                    break;
                case SINGLE:
                default:
                    tree = parser.parseStatement(sql);
                    break;
            }
            if (opts.pretty) {
                printTreeIndented(tree, DorisParser.ruleNames, System.out, 0);
            } else {
                System.out.println(Trees.toStringTree(tree, Arrays.asList(DorisParser.ruleNames)));
            }
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void printTreeIndented(ParseTree tree, String[] ruleNames, PrintStream out, int indent) {
        StringBuilder pad = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            pad.append("  ");
        }
        String label;
        if (tree instanceof ParserRuleContext) {
            label = ruleNames[((ParserRuleContext) tree).getRuleIndex()];
        } else if (tree instanceof TerminalNode) {
            label = "'" + tree.getText() + "'";
        } else {
            label = tree.getClass().getSimpleName();
        }
        out.println(pad.toString() + label);
        for (int i = 0; i < tree.getChildCount(); i++) {
            printTreeIndented(tree.getChild(i), ruleNames, out, indent + 1);
        }
    }

    private static String readSql(Options opts) throws IOException {
        if (opts.execSql != null) {
            return opts.execSql;
        }
        if (opts.file != null) {
            return new String(Files.readAllBytes(Paths.get(opts.file)), StandardCharsets.UTF_8);
        }
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }
        }
        return sb.toString();
    }

    private enum Mode { SINGLE, MULTI, EXPRESSION }

    private static final class Options {
        Mode mode = Mode.SINGLE;
        boolean pretty;
        boolean noBackslashEscapes;
        boolean ansiSql;
        boolean help;
        String execSql;
        String file;
    }

    private static Options parseArgs(String[] args) {
        Options o = new Options();
        int i = 0;
        while (i < args.length) {
            String a = args[i];
            switch (a) {
                case "-h":
                case "--help":
                    o.help = true;
                    return o;
                case "-e":
                case "--exec":
                    if (++i >= args.length) {
                        throw new UsageException("--exec requires a SQL argument");
                    }
                    o.execSql = args[i];
                    break;
                case "-f":
                case "--file":
                    if (++i >= args.length) {
                        throw new UsageException("--file requires a path");
                    }
                    o.file = args[i];
                    break;
                case "--multi":
                    o.mode = Mode.MULTI;
                    break;
                case "--expression":
                    o.mode = Mode.EXPRESSION;
                    break;
                case "--pretty":
                    o.pretty = true;
                    break;
                case "--no-backslash-escapes":
                    o.noBackslashEscapes = true;
                    break;
                case "--ansi":
                    o.ansiSql = true;
                    break;
                default:
                    if (o.execSql == null && o.file == null && !a.startsWith("-")) {
                        o.execSql = a;
                    } else {
                        throw new UsageException("unknown argument: " + a);
                    }
                    break;
            }
            i++;
        }
        return o;
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: doris-sql-parse [OPTIONS] [SQL]");
        out.println();
        out.println("Read a Doris SQL string and print its ANTLR parse tree (CST).");
        out.println();
        out.println("Input (mutually exclusive; reads from stdin if none of these are given):");
        out.println("  SQL                       SQL string as a positional argument");
        out.println("  -e, --exec <SQL>          SQL string");
        out.println("  -f, --file <PATH>         Read SQL from a UTF-8 text file");
        out.println();
        out.println("Parse mode:");
        out.println("  (default)                 Parse as a single statement");
        out.println("  --multi                   Parse as multiple statements (separated by ;)");
        out.println("  --expression              Parse as a single expression");
        out.println();
        out.println("Output:");
        out.println("  (default)                 ANTLR LISP-style toStringTree on one line");
        out.println("  --pretty                  Indented multi-line tree");
        out.println();
        out.println("Dialect flags:");
        out.println("  --no-backslash-escapes    MySQL NO_BACKSLASH_ESCAPES sql_mode behavior");
        out.println("  --ansi                    Enable ANSI SQL syntax variants");
        out.println();
        out.println("Other:");
        out.println("  -h, --help                Show this help");
        out.println();
        out.println("Exit codes: 0 success, 1 parse error, 2 usage or I/O error.");
    }

    private static final class UsageException extends RuntimeException {
        UsageException(String msg) {
            super(msg);
        }
    }
}
