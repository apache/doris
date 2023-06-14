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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/Copy.java
// and modified by Doris

package org.apache.doris.hplsql;

import org.apache.doris.hplsql.executor.QueryExecutor;
import org.apache.doris.hplsql.executor.QueryResult;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Copy {

    Exec exec;
    Timer timer = new Timer();
    boolean trace = false;
    boolean info = false;

    long srcSizeInBytes = 0;

    String delimiter = "\t";
    boolean sqlInsert = false;
    String sqlInsertName;
    String targetConn;
    int batchSize = 1000;

    boolean overwrite = false;
    boolean delete = false;
    boolean ignore = false;
    private QueryExecutor queryExecutor;

    Copy(Exec e, QueryExecutor queryExecutor) {
        exec = e;
        trace = exec.getTrace();
        info = exec.getInfo();
        this.queryExecutor = queryExecutor;
    }

    /**
     * Run COPY command
     */
    Integer run(org.apache.doris.hplsql.HplsqlParser.Copy_stmtContext ctx) {
        trace(ctx, "COPY");
        initOptions(ctx);
        StringBuilder sql = new StringBuilder();
        if (ctx.table_name() != null) {
            String table = evalPop(ctx.table_name()).toString();
            sql.append("SELECT * FROM ");
            sql.append(table);
        } else {
            sql.append(evalPop(ctx.select_stmt()).toString());
            if (trace) {
                trace(ctx, "Statement:\n" + sql);
            }
        }
        QueryResult query = queryExecutor.executeQuery(sql.toString(), ctx);
        if (query.error()) {
            exec.signal(query);
            return 1;
        }
        exec.setSqlSuccess();
        try {
            if (targetConn != null) {
                copyToTable(ctx, query);
            } else {
                copyToFile(ctx, query);
            }
        } catch (Exception e) {
            exec.signal(e);
            return 1;
        } finally {
            query.close();
        }
        return 0;
    }

    /**
     * Copy the query results to another table
     *
     * @throws Exception
     */
    void copyToTable(org.apache.doris.hplsql.HplsqlParser.Copy_stmtContext ctx, QueryResult query) throws Exception {
        int cols = query.columnCount();
        int rows = 0;
        if (trace) {
            trace(ctx, "SELECT executed: " + cols + " columns");
        }
        Connection conn = exec.getConnection(targetConn);
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO " + sqlInsertName + " VALUES (");
        for (int i = 0; i < cols; i++) {
            sql.append("?");
            if (i + 1 < cols) {
                sql.append(",");
            }
        }
        sql.append(")");
        PreparedStatement ps = conn.prepareStatement(sql.toString());
        long start = timer.start();
        long prev = start;
        boolean batchOpen = false;
        while (query.next()) {
            for (int i = 0; i < cols; i++) {
                ps.setObject(i, query.column(i, Object.class));
            }
            rows++;
            if (batchSize > 1) {
                ps.addBatch();
                batchOpen = true;
                if (rows % batchSize == 0) {
                    ps.executeBatch();
                    batchOpen = false;
                }
            } else {
                ps.executeUpdate();
            }
            if (trace && rows % 100 == 0) {
                long cur = timer.current();
                if (cur - prev > 10000) {
                    trace(ctx, "Copying rows: " + rows + " (" + rows / ((cur - start) / 1000) + " rows/sec)");
                    prev = cur;
                }
            }
        }
        if (batchOpen) {
            ps.executeBatch();
        }
        ps.close();
        exec.returnConnection(targetConn, conn);
        exec.setRowCount(rows);
        long elapsed = timer.stop();
        if (info) {
            DecimalFormat df = new DecimalFormat("#,##0.00");
            df.setRoundingMode(RoundingMode.HALF_UP);
            info(ctx, "COPY completed: " + rows + " row(s), " + timer.format() + ", " + df.format(
                    rows / (elapsed / 1000.0)) + " rows/sec");
        }
    }

    /**
     * Copy the query results to a file
     *
     * @throws Exception
     */
    void copyToFile(org.apache.doris.hplsql.HplsqlParser.Copy_stmtContext ctx, QueryResult query) throws Exception {
        String filename = null;
        if (ctx.copy_target().expr() != null) {
            filename = evalPop(ctx.copy_target().expr()).toString();
        } else {
            filename = ctx.copy_target().getText();
        }
        byte[] del = delimiter.getBytes();
        byte[] rowdel = "\n".getBytes();
        byte[] nullstr = "NULL".getBytes();
        int cols = query.columnCount();
        int rows = 0;
        long bytes = 0;
        if (trace || info) {
            String mes = "Query executed: " + cols + " columns, output file: " + filename;
            if (trace) {
                trace(ctx, mes);
            } else {
                info(ctx, mes);
            }
        }
        java.io.File file = null;
        File hdfsFile = null;
        if (ctx.T_HDFS() == null) {
            file = new java.io.File(filename);
        } else {
            hdfsFile = new File();
        }
        OutputStream out = null;
        timer.start();
        try {
            if (file != null) {
                if (!file.exists()) {
                    file.createNewFile();
                }
                out = new FileOutputStream(file, false /*append*/);
            } else {
                out = hdfsFile.create(filename, true /*overwrite*/);
            }
            String col;
            String sql = "";
            if (sqlInsert) {
                sql = "INSERT INTO " + sqlInsertName + " VALUES (";
                rowdel = ");\n".getBytes();
            }
            while (query.next()) {
                if (sqlInsert) {
                    out.write(sql.getBytes());
                }
                for (int i = 0; i < cols; i++) {
                    if (i > 0) {
                        out.write(del);
                        bytes += del.length;
                    }
                    col = query.column(i, String.class);
                    if (col != null) {
                        if (sqlInsert) {
                            col = org.apache.doris.hplsql.Utils.quoteString(col);
                        }
                        byte[] b = col.getBytes();
                        out.write(b);
                        bytes += b.length;
                    } else if (sqlInsert) {
                        out.write(nullstr);
                    }
                }
                out.write(rowdel);
                bytes += rowdel.length;
                rows++;
            }
            exec.setRowCount(rows);
        } finally {
            if (out != null) {
                out.close();
            }
        }
        long elapsed = timer.stop();
        if (info) {
            DecimalFormat df = new DecimalFormat("#,##0.00");
            df.setRoundingMode(RoundingMode.HALF_UP);
            info(ctx, "COPY completed: " + rows + " row(s), "
                    + org.apache.doris.hplsql.Utils.formatSizeInBytes(bytes) + ", "
                    + timer.format() + ", " + df.format(rows / (elapsed / 1000.0)) + " rows/sec");
        }
    }

    /**
     * Run COPY FROM LOCAL statement
     */
    public Integer runFromLocal(org.apache.doris.hplsql.HplsqlParser.Copy_from_local_stmtContext ctx) {
        trace(ctx, "COPY FROM LOCAL");
        initFileOptions(ctx.copy_file_option());
        HashMap<String, Pair<String, Long>> srcFiles = new HashMap<>();
        String src = evalPop(ctx.copy_source(0)).toString();
        String dest = evalPop(ctx.copy_target()).toString();
        int srcItems = ctx.copy_source().size();
        for (int i = 0; i < srcItems; i++) {
            createLocalFileList(srcFiles, evalPop(ctx.copy_source(i)).toString(), null);
        }
        if (info) {
            info(ctx, "Files to copy: " + srcFiles.size() + " ("
                    + org.apache.doris.hplsql.Utils.formatSizeInBytes(srcSizeInBytes) + ")");
        }
        if (srcFiles.size() == 0) {
            exec.setHostCode(2);
            return 2;
        }
        timer.start();
        File file = new File();
        FileSystem fs;
        int succeed = 0;
        int failed = 0;
        long copiedSize = 0;
        try {
            fs = file.createFs();
            boolean multi = false;
            if (srcFiles.size() > 1) {
                multi = true;
            }
            for (Map.Entry<String, Pair<String, Long>> i : srcFiles.entrySet()) {
                try {
                    Path s = new Path(i.getKey());
                    Path d;
                    if (multi) {
                        String relativePath = i.getValue().getLeft();
                        if (relativePath == null) {
                            d = new Path(dest, s.getName());
                        } else {
                            d = new Path(dest, relativePath + Path.SEPARATOR + s.getName());
                        }
                    } else {
                        // Path to file is specified (can be relative),
                        // so treat target as a file name (hadoop fs -put behavior)
                        if (srcItems == 1 && i.getKey().endsWith(src)) {
                            d = new Path(dest);
                        } else { // Source directory is specified, so treat the target as a directory
                            d = new Path(dest + Path.SEPARATOR + s.getName());
                        }
                    }
                    fs.copyFromLocalFile(delete, overwrite, s, d);
                    succeed++;
                    long size = i.getValue().getRight();
                    copiedSize += size;
                    if (info) {
                        info(ctx, "Copied: " + file.resolvePath(d) + " ("
                                + org.apache.doris.hplsql.Utils.formatSizeInBytes(size) + ")");
                    }
                } catch (IOException e) {
                    failed++;
                    if (!ignore) {
                        throw e;
                    }
                }
            }
        } catch (IOException e) {
            exec.signal(e);
            exec.setHostCode(1);
            return 1;
        } finally {
            long elapsed = timer.stop();
            if (info) {
                info(ctx, "COPY completed: " + succeed + " succeed, " + failed + " failed, "
                        + timer.format() + ", " + org.apache.doris.hplsql.Utils.formatSizeInBytes(copiedSize) + ", "
                        + org.apache.doris.hplsql.Utils.formatBytesPerSec(copiedSize, elapsed));
            }
            if (failed == 0) {
                exec.setHostCode(0);
            } else {
                exec.setHostCode(1);
            }
            file.close();
        }
        return 0;
    }

    /**
     * Create the list of local files for the specified path (including subdirectories)
     */
    void createLocalFileList(HashMap<String, Pair<String, Long>> list, String path, String relativePath) {
        java.io.File file = new java.io.File(path);
        if (file.exists()) {
            if (file.isDirectory()) {
                for (java.io.File i : file.listFiles()) {
                    if (i.isDirectory()) {
                        String rel;
                        if (relativePath == null) {
                            rel = i.getName();
                        } else {
                            rel = relativePath + java.io.File.separator + i.getName();
                        }
                        createLocalFileList(list, i.getAbsolutePath(), rel);
                    } else {
                        long size = i.length();
                        list.put(i.getAbsolutePath(), Pair.of(relativePath, size));
                        srcSizeInBytes += size;
                    }
                }
            } else {
                long size = file.length();
                list.put(file.getAbsolutePath(), Pair.of(relativePath, size));
                srcSizeInBytes += size;
            }
        }
    }

    /**
     * Initialize COPY command options
     */
    void initOptions(org.apache.doris.hplsql.HplsqlParser.Copy_stmtContext ctx) {
        int cnt = ctx.copy_option().size();
        for (int i = 0; i < cnt; i++) {
            org.apache.doris.hplsql.HplsqlParser.Copy_optionContext option = ctx.copy_option(i);
            if (option.T_DELIMITER() != null) {
                delimiter = StringEscapeUtils.unescapeJava(evalPop(option.expr()).toString());
            } else if (option.T_SQLINSERT() != null) {
                sqlInsert = true;
                delimiter = ", ";
                if (option.qident() != null) {
                    sqlInsertName = option.qident().getText();
                }
            } else if (option.T_AT() != null) {
                targetConn = option.qident().getText();
                if (ctx.copy_target().expr() != null) {
                    sqlInsertName = evalPop(ctx.copy_target().expr()).toString();
                } else {
                    sqlInsertName = ctx.copy_target().getText();
                }
            } else if (option.T_BATCHSIZE() != null) {
                batchSize = evalPop(option.expr()).intValue();
            }
        }
    }

    /**
     * Initialize COPY FILE options
     */
    void initFileOptions(List<org.apache.doris.hplsql.HplsqlParser.Copy_file_optionContext> options) {
        srcSizeInBytes = 0;
        for (org.apache.doris.hplsql.HplsqlParser.Copy_file_optionContext i : options) {
            if (i.T_OVERWRITE() != null) {
                overwrite = true;
            } else if (i.T_DELETE() != null) {
                delete = true;
            } else if (i.T_IGNORE() != null) {
                ignore = true;
            }
        }
    }

    /**
     * Evaluate the expression and pop value from the stack
     */
    Var evalPop(ParserRuleContext ctx) {
        exec.visit(ctx);
        if (!exec.stack.isEmpty()) {
            return exec.stackPop();
        }
        return Var.Empty;
    }

    /**
     * Trace and information
     */
    public void trace(ParserRuleContext ctx, String message) {
        exec.trace(ctx, message);
    }

    public void info(ParserRuleContext ctx, String message) {
        exec.info(ctx, message);
    }
}
