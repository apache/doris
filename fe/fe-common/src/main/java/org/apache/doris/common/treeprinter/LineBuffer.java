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

// Author: https://github.com/davidsusu/tree-printer

package org.apache.doris.common.treeprinter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LineBuffer {

    private final Appendable out;

    private int flushedRowCount = 0;
    
    private List<String> lines = new ArrayList<String>();
    
    public LineBuffer(Appendable out) {
        this.out = out;
    }
    
    public void write(int row, int col, String text) {
        String[] textLines = text.split("\n");
        int lineCount = textLines.length;
        for (int i = 0; i < lineCount; i++) {
            writeLine(row + i, col, textLines[i]);
        }
    }

    public void flush() {
        flush(flushedRowCount + lines.size());
    }
    
    public void flush(int rows) {
        try {
            flushThrows(rows);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void flushThrows(int rows) throws IOException {
        if (rows <= flushedRowCount) {
            return;
        }
        
        int currentLineCount = lines.size();
        int deleteLineCount = rows - flushedRowCount;
        if (currentLineCount <= deleteLineCount) {
            for (String line: lines) {
                out.append(line + "\n");
            }
            lines.clear();
        } else {
            for (int i = 0; i < deleteLineCount; i++) {
                String line = lines.get(i);
                out.append(line + "\n");
            }
            lines = new ArrayList<String>(lines.subList(deleteLineCount, currentLineCount));
        }
        
        flushedRowCount = rows;
    }
    
    private void writeLine(int row, int col, String textLine) {
        if (row < flushedRowCount) {
            return;
        }
        int currentLineCount = lines.size();
        int lineIndex = row - flushedRowCount;
        String originalLine;
        if (lineIndex < currentLineCount) {
            originalLine = lines.get(lineIndex);
        } else {
            for (int i = currentLineCount; i <= lineIndex; i++) {
                lines.add("");
            }
            originalLine = "";
        }
        String newLine = writeIntoLine(originalLine, col, textLine);
        lines.set(lineIndex, newLine);
    }
    
    private String writeIntoLine(String contextLine, int pos, String textLine) {
        String beforeContent;
        String beforePad;

        int contextLineLength = contextLine.length();
        
        if (contextLineLength <= pos) {
            beforeContent = contextLine;
            beforePad = Util.repeat(' ', pos - contextLineLength);
        } else {
            beforeContent = contextLine.substring(0, pos);
            beforePad = "";
        }

        int textLineLength = textLine.length();
        
        String afterContent;
        
        if (pos + textLineLength < contextLineLength) {
            afterContent = contextLine.substring(pos + textLineLength);
        } else {
            afterContent = "";
        }
        
        return beforeContent + beforePad + textLine + afterContent;
    }
    
}
