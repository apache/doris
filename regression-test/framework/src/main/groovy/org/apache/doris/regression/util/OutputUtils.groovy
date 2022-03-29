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

package org.apache.doris.regression.util

import com.google.common.collect.ImmutableList
import groovy.transform.CompileStatic
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
import org.apache.commons.csv.CSVRecord
import org.apache.commons.io.LineIterator

import java.sql.Connection
import java.sql.ResultSet

import static org.apache.doris.regression.util.DataUtils.sortByToString

@CompileStatic
class OutputUtils {
    static String toCsvString(Object cell) {
        StringWriter writer = new StringWriter()
        def printer = new CSVPrinter(new PrintWriter(writer), CSVFormat.MYSQL)
        printer.print(cell)
        return writer.toString()
    }

    static String toCsvString(List<Object> row) {
        StringWriter writer = new StringWriter()
        def printer = new CSVPrinter(new PrintWriter(writer), CSVFormat.MYSQL)
        for (int i = 0; i < row.size(); ++i) {
            printer.print(row.get(i))
        }
        return writer.toString()
    }

    static String checkOutput(Iterator<List<String>> expectIterator, Connection conn, String sql, boolean order, String info) {
        conn.prepareStatement(sql).withCloseable { stmt ->
            boolean hasResultSet = stmt.execute()
            if (!hasResultSet) {
                if(expectIterator.hasNext()) {
                    return "${info}, result mismatch, real row number is 0, but expect more than it"
                }
            } else {
                def columnCount = stmt.resultSet.metaData.columnCount

                List<List<Object>> rows = new ArrayList<>()
                while(stmt.resultSet.next()) {
                    def row = new ArrayList<>()
                    for (int i = 1; i <= columnCount; ++i) {
                        row.add(stmt.resultSet.getObject(i))
                    }
                    rows.add(row)
                }
                if (order) {
                    rows = sortByToString(rows)
                }

                for(int i = 1; i <= rows.size(); i++) {
                    if(expectIterator.hasNext()) {
                        List<String> expect_row = expectIterator.next()
                        for(int j = 1; j <= columnCount; j++) {
                            String expect_cell = toCsvString(expect_row[j-1])
                            String real_cell = toCsvString(rows[i-1][j-1])
                            String tp = stmt.resultSet.metaData.getColumnTypeName(j)
                            if(tp == "FLOAT" || tp == "DOUBLE") {
                                double expect_double = Double.parseDouble(expect_cell)
                                double real_double = Double.parseDouble(real_cell)
                                
                                double real_relative_error = Math.abs(expect_double - real_double) / real_double
                                double expect_relative_error = 1e-10

                                if(expect_relative_error < real_relative_error) {
                                    return "${info}, ${tp} result mismatch.\nExpect cell is: ${expect_cell}\nBut real is: ${real_cell}\nrelative error is: ${real_relative_error}, bigger than ${expect_relative_error}"
                                }
                            } else if(tp == "DATE" || tp =="DATETIME") {
                                expect_cell = expect_cell.replace("T", " ")
                                real_cell = real_cell.replace("T", " ")

                                if(!expect_cell.equals(real_cell)) {
                                    return "${info}, ${tp} result mismatch.\nExpect cell is: ${expect_cell}\nBut real is: ${real_cell}"
                                }
                            } else {
                                if(!expect_cell.equals(real_cell)) {
                                    return "${info}, ${tp} result mismatch.\nExpect cell is: ${expect_cell}\nBut real is: ${real_cell}"
                                }
                            }
                        }
                    } else {
                        return "${info}, result mismatch, real row number is ${rows.size()}, but expect less than it"
                    }
                }
                if(expectIterator.hasNext()) {
                    return "${info}, result mismatch, real row number is ${rows.size()}, but expect less than it"
                }
            }
        }
    }

    static OutputBlocksIterator iterator(File file) {
        def it = new ReusableIterator<String>(new LineIteratorAdaptor(new LineIterator(new FileReader(file))))
        return new OutputBlocksIterator(it)
    }

    static OutputBlocksWriter writer(File file) {
        return new OutputBlocksWriter(file)
    }

    static class LineIteratorAdaptor implements CloseableIterator<String> {
        LineIterator lineIt

        LineIteratorAdaptor(LineIterator lineIt) {
            this.lineIt = lineIt
        }

        @Override
        void close() throws IOException {
            lineIt.close()
        }

        @Override
        boolean hasNext() {
            return lineIt.hasNext()
        }

        @Override
        String next() {
            return lineIt.next()
        }
    }

    static class OutputBlocksWriter {
        private PrintWriter writer

        OutputBlocksWriter(File file) {
            if (file != null) {
                writer = file.newPrintWriter()
                writer.println("""-- This file is automatically generated. You should know what you did if you want to edit this""")
            }
        }

        synchronized void write(Iterator<List<String>> real, String comment) {
            if (writer != null) {
                writer.println("-- !${comment} --")
                while (real.hasNext()) {
                    writer.println(toCsvString(real.next() as List<Object>))
                }
                writer.println()
            }
        }

        synchronized void close() {
            if (writer != null) {
                writer.close()
            }
        }
    }

    static class TagBlockIterator implements Iterator<List<String>> {
        private final String tag
        private Iterator<List<String>> it

        TagBlockIterator(String tag, Iterator<List<String>> it) {
            this.tag = tag
            this.it = it
        }

        String getTag() {
            return tag
        }

        @Override
        boolean hasNext() {
            return it.hasNext()
        }

        @Override
        List<String> next() {
            return it.next()
        }
    }

    static class OutputBlocksIterator implements CloseableIterator<TagBlockIterator> {
        private ReusableIterator<String> lineIt
        private TagBlockIterator cache
        private boolean cached

        OutputBlocksIterator(ReusableIterator<String> lineIt) {
            this.lineIt = lineIt
        }

        @Override
        void close() throws IOException {
            lineIt.close()
        }

        @Override
        boolean hasNext() {
            if (!cached) {
                if (cache != null) {
                    while (cache.hasNext()) {
                        cache.next()
                    }
                }
                if (!lineIt.hasNext()) {
                    return false
                }

                String tag = null
                // find next comment block
                while (true) {
                    String blockComment = lineIt.next() // skip block comment, e.g. -- !qt_sql_1 --
                    if (blockComment.startsWith("-- !") && blockComment.endsWith(" --")) {
                        if (blockComment.startsWith("-- !")) {
                            tag = blockComment.substring("-- !".length(), blockComment.length() - " --".length()).trim()
                        }
                        break
                    }
                    if (!lineIt.hasNext()) {
                        return false
                    }
                }
                cache = new TagBlockIterator(tag, new CsvParserIterator(new SkipLastEmptyLineIterator(new OutputBlockIterator(lineIt))))
                cached = true
                return true
            } else {
                return true
            }
        }

        boolean hasNextTagBlock(String tag) {
            while (hasNext()) {
                if (Objects.equals(tag, cache.tag)) {
                    return true
                }

                // drain out
                def it = next()
                while (it.hasNext()) {
                    it.next()
                }
            }
            return false
        }

        @Override
        TagBlockIterator next() {
            if (hasNext()) {
                cached = false
                return cache
            }
            throw new NoSuchElementException()
        }
    }

    static class CsvParserIterator implements Iterator<List<String>> {
        private Iterator<String> it

        CsvParserIterator(Iterator<String> it) {
            this.it = it
        }

        @Override
        boolean hasNext() {
            return it.hasNext()
        }

        @Override
        List<String> next() {
            String line = it.next()
            if (line.size() == 0) {
                return ImmutableList.of(line)
            }
            CSVRecord record = CSVFormat.MYSQL.parse(new StringReader(line)).first()
            List<String> row = new ArrayList(record.size())
            for (int i = 0; i < record.size(); ++i) {
                row.add(record.get(i))
            }
            return row
        }
    }

    static class SkipLastEmptyLineIterator implements Iterator<String> {
        private Iterator<String> it
        private String cache
        private boolean cached

        SkipLastEmptyLineIterator(Iterator<String> it) {
            this.it = it
        }

        @Override
        boolean hasNext() {
            if (!cached) {
                if (!it.hasNext()) {
                    return false
                }
                String next = it.next()
                if (next.length() == 0 && !it.hasNext()) {
                    return false
                }
                cache = next
                cached = true
                return true
            } else {
                return true
            }
        }

        @Override
        String next() {
            if (hasNext()) {
                cached = false
                return cache
            }
            throw new NoSuchElementException()
        }
    }

    static class OutputBlockIterator implements Iterator<String> {
        private ReusableIterator<String> it

        OutputBlockIterator(ReusableIterator<String> it) {
            this.it = it
        }

        @Override
        boolean hasNext() {
            while (true) {
                if (!it.hasNext()) {
                    return false
                }
                // predict next line
                String line = it.preRead()
                if (line.startsWith("-- !") && line.endsWith(" --")) {
                    return false
                } else if (line.startsWith("-- ")) {
                    it.next()
                    continue
                }
                return true
            }
            return false
        }

        @Override
        String next() {
            if (hasNext()) {
                return it.next()
            }
            throw new NoSuchElementException()
        }
    }
}
