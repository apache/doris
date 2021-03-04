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

public abstract class AbstractTreePrinter implements TreePrinter {

    @Override
    public void print(TreeNode rootNode) {
        print(rootNode, System.out);
    }

    @Override
    public String getAsString(TreeNode rootNode) {
        StringBuilder builder = new StringBuilder();
        print(rootNode, builder);
        return builder.toString();
    }
    
    protected void write(Appendable out, String content) {
        try {
            out.append(content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void writeln(Appendable out, String content) {
        write(out, content + "\n");
    }
    
}
