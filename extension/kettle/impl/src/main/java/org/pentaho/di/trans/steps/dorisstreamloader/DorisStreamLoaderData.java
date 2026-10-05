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

package org.pentaho.di.trans.steps.dorisstreamloader;

import org.pentaho.di.core.database.Database;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.steps.dorisstreamloader.serializer.DorisRecordSerializer;

/**
 * DorisStreamLoaderData
 */
public class DorisStreamLoaderData extends BaseStepData implements StepDataInterface {
  public Database db;

  public int[] keynrs; // nr of keylookup -value in row...
  public ValueMetaInterface[] formatMeta;
  public String[] fieldNames;

  public DorisRecordSerializer serializer;
  /**
   * Default constructor.
   */
  public DorisStreamLoaderData() {
    super();

    db = null;
  }
}
