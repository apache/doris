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

package org.apache.doris.nereids.trees.expressions.visitor;

import org.apache.doris.nereids.trees.expressions.functions.table.Backends;
import org.apache.doris.nereids.trees.expressions.functions.table.Catalogs;
import org.apache.doris.nereids.trees.expressions.functions.table.Frontends;
import org.apache.doris.nereids.trees.expressions.functions.table.FrontendsDisks;
import org.apache.doris.nereids.trees.expressions.functions.table.GroupCommit;
import org.apache.doris.nereids.trees.expressions.functions.table.Hdfs;
import org.apache.doris.nereids.trees.expressions.functions.table.IcebergMeta;
import org.apache.doris.nereids.trees.expressions.functions.table.Local;
import org.apache.doris.nereids.trees.expressions.functions.table.Numbers;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.expressions.functions.table.WorkloadGroups;

/** TableValuedFunctionVisitor */
public interface TableValuedFunctionVisitor<R, C> {
    R visitTableValuedFunction(TableValuedFunction tableValuedFunction, C context);

    default R visitBackends(Backends backends, C context) {
        return visitTableValuedFunction(backends, context);
    }

    default R visitCatalogs(Catalogs catalogs, C context) {
        return visitTableValuedFunction(catalogs, context);
    }

    default R visitFrontends(Frontends frontends, C context) {
        return visitTableValuedFunction(frontends, context);
    }

    default R visitFrontendsDisks(FrontendsDisks frontendsDisks, C context) {
        return visitTableValuedFunction(frontendsDisks, context);
    }

    default R visitGroupCommit(GroupCommit groupCommit, C context) {
        return visitTableValuedFunction(groupCommit, context);
    }

    default R visitHdfs(Hdfs hdfs, C context) {
        return visitTableValuedFunction(hdfs, context);
    }

    default R visitIcebergMeta(IcebergMeta icebergMeta, C context) {
        return visitTableValuedFunction(icebergMeta, context);
    }

    default R visitLocal(Local local, C context) {
        return visitTableValuedFunction(local, context);
    }

    default R visitNumbers(Numbers numbers, C context) {
        return visitTableValuedFunction(numbers, context);
    }

    default R visitS3(S3 s3, C context) {
        return visitTableValuedFunction(s3, context);
    }

    default R visitWorkloadGroups(WorkloadGroups workloadGroups, C context) {
        return visitTableValuedFunction(workloadGroups, context);
    }
}
