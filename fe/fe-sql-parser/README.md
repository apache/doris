# Doris FE SQL Parser

`fe-sql-parser` is a standalone ANTLR4-based syntax parser for Apache Doris SQL. It produces an ANTLR parse tree (CST) for any Doris-dialect SQL string. It performs **no semantic analysis**: identifiers are not resolved, tables and columns are not validated, and types are not checked. The module has a single runtime dependency: `org.antlr:antlr4-runtime`.

The module is the single source of truth for the Doris SQL grammar; `fe-core` consumes the parser through this module rather than maintaining its own copy.

## Module Layout

```
fe-sql-parser/
├── pom.xml
├── src/main/antlr4/org/apache/doris/nereids/
│   ├── DorisLexer.g4               # Doris SQL lexer grammar
│   └── DorisParser.g4              # Doris SQL parser grammar
└── src/main/java/
    ├── org/apache/doris/nereids/
    │   ├── parser/                 # Parser support: CaseInsensitiveStream,
    │   │                           # Origin, OriginAware, ParserUtils,
    │   │                           # ParseErrorListener, PostProcessor
    │   ├── exceptions/             # ParseException, SyntaxParseException
    │   └── errors/QueryParsingErrors.java
    └── org/apache/doris/sqlparser/
        ├── DorisSqlParser.java     # Public library facade
        └── DorisSqlParserCli.java  # Command-line entry point
```

At build time the ANTLR Maven plugin generates `org.apache.doris.nereids.DorisLexer`, `DorisParser`, `DorisParserBaseVisitor`, and `DorisParserBaseListener` into `target/generated-sources/antlr4/`.

## Build

The module has two build modes: the default mode produces a thin library jar that `fe-core` and downstream tools depend on; the `cli` profile additionally produces a self-contained executable jar.

### Library jar (default build)

```bash
# From the fe/ directory
mvn -pl fe-sql-parser -am package
```

Output: `fe/fe-sql-parser/target/doris-fe-sql-parser.jar` (~1.3 MB). This jar contains only the parser classes; it expects `org.antlr:antlr4-runtime:4.13.1` to be provided by the consuming project's classpath.

To install it to your local Maven repository so other projects can resolve it:

```bash
mvn -pl fe-sql-parser -am -Pflatten install -DskipTests
```

The `flatten` profile is required so the installed POM has `${revision}` resolved to a concrete version.

### Standalone CLI jar

```bash
# From the fe/ directory
mvn -pl fe-sql-parser -Pcli package -DskipTests
```

Output: `fe/fe-sql-parser/target/fe-sql-parser-1.2-SNAPSHOT-cli.jar` (~1.7 MB).

This is a self-contained executable jar produced by `maven-shade-plugin`:

- Bundles `antlr4-runtime` so the jar runs anywhere with a JRE 8+
- Manifest sets `Main-Class: org.apache.doris.sqlparser.DorisSqlParserCli`
- `<minimizeJar>true</minimizeJar>` strips unused classes (transitively-inherited logging, test utilities, etc.) so the final jar contains only the parser plus its actual reachable dependencies

The CLI profile is gated so default Doris builds do not pay the shading cost. The thin library jar produced by the default build is unaffected — `fe-core` continues to consume it directly.

## CLI Usage

```
java -jar fe-sql-parser-1.2-SNAPSHOT-cli.jar [OPTIONS] [SQL]
```

### Input sources (mutually exclusive)

| Source | Example |
|--------|---------|
| Positional argument | `java -jar ...-cli.jar "SELECT 1"` |
| `-e` / `--exec <SQL>` | `java -jar ...-cli.jar -e "SELECT 1"` |
| `-f` / `--file <PATH>` | `java -jar ...-cli.jar -f query.sql` |
| stdin (when none of the above) | `echo "SELECT 1" \| java -jar ...-cli.jar` |

### Parse modes

| Flag | Grammar rule | Use case |
|------|--------------|----------|
| (default) | `singleStatement` | One SQL statement |
| `--multi` | `multiStatements` | Multiple statements separated by `;` |
| `--expression` | `expressionWithEof` | A single SQL expression |

### Output formats

| Flag | Output |
|------|--------|
| (default) | ANTLR LISP-style tree on one line |
| `--pretty` | Indented multi-line tree, two-space indent per level |

### Dialect flags

| Flag | Effect |
|------|--------|
| `--no-backslash-escapes` | Maps to MySQL's `NO_BACKSLASH_ESCAPES` sql_mode — backslash is not a string-literal escape character |
| `--ansi` | Enables ANSI SQL syntax variants in the few grammar rules that branch on it |

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Parse succeeded |
| 1 | Parse failed — `ParseException` thrown; the error message is printed to stderr with the offending line/column and a `^^^` pointer |
| 2 | Usage error or I/O error (bad flag, unreadable file, empty input) |

### Examples

Single statement, default LISP format:

```bash
$ java -jar ...-cli.jar "SELECT 1"
(singleStatement (statement (statementBase (query (queryTerm (queryPrimary
(querySpecification (selectClause SELECT (selectColumnClause (namedExpressionSeq
(namedExpression (expression (booleanExpression (valueExpression (primaryExpression
(constant (number 1)))))))))) queryOrganization))) queryOrganization))) <EOF>)
```

Single statement, pretty format:

```bash
$ java -jar ...-cli.jar --pretty "SELECT a FROM t WHERE a > 1"
singleStatement
  statement
    statementBase
      query
        queryTerm
          queryPrimary
            querySpecification
              selectClause
                'SELECT'
                ...
              fromClause
                'FROM'
                ...
              whereClause
                'WHERE'
                ...
  '<EOF>'
```

Multiple statements:

```bash
$ java -jar ...-cli.jar --multi "USE db1; SELECT 1; SELECT 2"
```

Single expression:

```bash
$ java -jar ...-cli.jar --expression "a + 1 * COALESCE(b, 0)"
```

From file:

```bash
$ java -jar ...-cli.jar -f path/to/my-query.sql
```

From stdin (pipe a heredoc or another command's output):

```bash
$ cat my-query.sql | java -jar ...-cli.jar
```

Parse error — note the non-zero exit code:

```bash
$ java -jar ...-cli.jar "SELEKT 1"
mismatched input 'SELEKT' expecting {...}(line 1, pos 0)
$ echo $?
1
```

### Shell wrapper (optional)

For frequent use, drop a wrapper on your `PATH`:

```bash
# ~/bin/doris-sql-parse
#!/usr/bin/env bash
exec java -jar /path/to/fe-sql-parser-1.2-SNAPSHOT-cli.jar "$@"
```

```bash
chmod +x ~/bin/doris-sql-parse
doris-sql-parse --pretty "SELECT 1"
```

## Library Usage

If you want to embed the parser in another JVM application rather than shelling out to the CLI.

### Maven dependency

```xml
<dependency>
    <groupId>org.apache.doris</groupId>
    <artifactId>fe-sql-parser</artifactId>
    <version>1.2-SNAPSHOT</version>
</dependency>
<!-- antlr4-runtime is pulled in transitively; declare it explicitly if you
     want to pin a specific version -->
<dependency>
    <groupId>org.antlr</groupId>
    <artifactId>antlr4-runtime</artifactId>
    <version>4.13.1</version>
</dependency>
```

Until the artifact is published to a public repository you need to `mvn install` it locally (see [Library jar](#library-jar-default-build) above).

### Minimal example

```java
import org.apache.doris.sqlparser.DorisSqlParser;
import org.apache.doris.nereids.DorisParser.SingleStatementContext;

DorisSqlParser parser = new DorisSqlParser();
SingleStatementContext tree = parser.parseStatement("SELECT a, b FROM t WHERE a > 1");
// `tree` is a standard ANTLR ParseTree; walk it with a Visitor or Listener.
```

### Walking the tree with a Visitor

```java
import org.apache.doris.nereids.DorisParser;
import org.apache.doris.nereids.DorisParserBaseVisitor;

import java.util.ArrayList;
import java.util.List;

DorisSqlParser parser = new DorisSqlParser();
SingleStatementContext tree = parser.parseStatement(
        "SELECT u.id FROM users u JOIN orders o ON u.id = o.uid");

List<String> tables = new ArrayList<>();
new DorisParserBaseVisitor<Void>() {
    @Override
    public Void visitTableName(DorisParser.TableNameContext ctx) {
        tables.add(ctx.multipartIdentifier().getText());
        return super.visitTableName(ctx);
    }
}.visit(tree);

System.out.println(tables);   // [users, orders]
```

`DorisParserBaseVisitor<T>` and `DorisParserBaseListener` are generated by ANTLR — every grammar rule has a corresponding `visitXxx` / `enterXxx` / `exitXxx` method you can override.

### Error handling

`ParseException` is a `RuntimeException`. You do not have to declare or catch it, but you usually want to:

```java
import org.apache.doris.nereids.exceptions.ParseException;

try {
    parser.parseStatement("SELEKT 1");
} catch (ParseException e) {
    // e.getMessage() includes "line N, pos M" and a `^^^` pointer into the SQL.
    System.err.println(e.getMessage());
}
```

### Lexer-only / token-level work

If you only need tokens (SQL formatter, comment extractor, keyword finder, hint inspector), skip the parser:

```java
import org.apache.doris.nereids.DorisLexer;
import org.antlr.v4.runtime.Token;

DorisSqlParser parser = new DorisSqlParser();
DorisLexer lexer = parser.newLexer("SELECT /*+ HINT */ a FROM t");
Token token;
while ((token = lexer.nextToken()).getType() != Token.EOF) {
    System.out.printf("%-20s %s%n",
            DorisLexer.VOCABULARY.getSymbolicName(token.getType()),
            token.getText());
}
```

## Configuration Knobs

`DorisSqlParser` is configured via constructor flags. Both default to `false`, which matches the most common Doris query behavior.

```java
DorisSqlParser parser = new DorisSqlParser(
    /* noBackslashEscapes = */ false,
    /* ansiSqlSyntax     = */ false
);
```

| Flag | Effect |
|------|--------|
| `noBackslashEscapes` | When `true`, `\` inside string literals is a literal backslash rather than an escape character. Matches MySQL's `NO_BACKSLASH_ESCAPES` sql_mode. |
| `ansiSqlSyntax` | When `true`, enables ANSI SQL behavior in a small number of grammar rules (mainly around `GROUP BY` / `ORDER BY` resolution). Matches the `enable_ansi_query_organization_behavior` Doris session variable. |

## Performance Notes

### Origin tracking fast path

`ParserUtils.withOrigin` pushes the current ANTLR rule's line/column onto a per-thread stack so that `ParseException` can report the exact source location of any error raised during tree construction. By default this uses a `ThreadLocal`; threads that run the parser on a hot path can opt into a faster field-based storage by implementing `org.apache.doris.nereids.parser.OriginAware`:

```java
public class MyParserThread extends Thread implements OriginAware {
    private Origin origin;
    @Override public Origin getOrigin() { return origin; }
    @Override public void setOrigin(Origin o) { this.origin = o; }
}
```

Any thread that does not implement `OriginAware` falls back to the `ThreadLocal` path. Correctness is identical either way; the fast path saves one `ThreadLocal` hash lookup per `withOrigin` call.

### Thread safety

`DorisSqlParser` is stateless aside from its constructor flags and can be reused as a shared singleton across threads. Each parse call constructs a fresh `Lexer`, `TokenStream`, and `Parser` internally.

## Caveats

- The grammar covers the full Doris SQL surface (DDL + DML + administrative commands). If you only care about `SELECT`, you still parse with the full parser and just visit the relevant subtree.
- No semantic analysis: identifiers like `t`, `a`, `u.id` come back as syntactic tokens. Resolving them against a catalog requires additional logic in your application.
- `antlr4-runtime:4.13.1` is a transitive dependency of the thin jar. Align with this version in your project or you will hit `NoSuchMethodError` at runtime.
- The CLI jar bundles `antlr4-runtime` so it has no classpath conflicts when run with `java -jar`.
- The module is not yet published to Maven Central. Until it is, consumers need to install it locally with `mvn install -Pflatten` or pull it from an internal repository.
