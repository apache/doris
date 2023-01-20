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
//
// Copied from
// https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore/blob/branch-3.4.0/
//

package com.amazonaws.glue.catalog.util;

import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.InvalidInputException;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public final class PartitionUtils {

  public static Map<PartitionKey, Partition> buildPartitionMap(final List<Partition> partitions) {
    Map<PartitionKey, Partition> partitionValuesMap = Maps.newHashMap();
    for (Partition partition : partitions) {
      partitionValuesMap.put(new PartitionKey(partition), partition);
    }
    return partitionValuesMap;
  }

  public static List<PartitionValueList> getPartitionValuesList(final Map<PartitionKey, Partition> partitionMap) {
    List<PartitionValueList> partitionValuesList = Lists.newArrayList();
    for (Map.Entry<PartitionKey, Partition> entry : partitionMap.entrySet()) {
      partitionValuesList.add(new PartitionValueList().withValues(entry.getValue().getValues()));
    }
    return partitionValuesList;
  }

  public static boolean isInvalidUserInputException(Exception e) {
    // exceptions caused by invalid requests, in which case we know all partitions creation failed
    return e instanceof EntityNotFoundException || e instanceof InvalidInputException;
  }

}
