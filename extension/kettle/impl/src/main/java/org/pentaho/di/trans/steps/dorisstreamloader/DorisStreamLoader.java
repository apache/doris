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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisBatchStreamLoad;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisOptions;
import org.pentaho.di.trans.steps.dorisstreamloader.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.CSV;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.FIELD_DELIMITER_DEFAULT;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.FIELD_DELIMITER_KEY;
import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.FORMAT_KEY;

/**
 * Doris Stream Load
 */
public class DorisStreamLoader extends BaseStep implements StepInterface {
  private static Class<?> PKG = DorisStreamLoaderMeta.class; // for i18n purposes, needed by Translator2!!
  private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoader.class);
  private DorisStreamLoaderMeta meta;
  private DorisStreamLoaderData data;
  private DorisBatchStreamLoad streamLoad;
  private DorisOptions options;

  public DorisStreamLoader(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                           Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    meta = (DorisStreamLoaderMeta) smi;
    data = (DorisStreamLoaderData) sdi;

    try {
      Object[] r = getRow(); // Get row from input rowset & set row busy!

      if ( r == null ) { // no more input to be expected...
        setOutputDone();
        closeOutput();
        return false;
      }
      if ( first ) {
        first = false;
        // Cache field indexes.
        data.keynrs = new int[meta.getFieldStream().length];
        for ( int i = 0; i < data.keynrs.length; i++ ) {
          data.keynrs[i] = getInputRowMeta().indexOfValue( meta.getFieldStream()[i] );
        }
        data.formatMeta = new ValueMetaInterface[data.keynrs.length];
        for ( int i = 0; i < data.keynrs.length; i++ ) {
          ValueMetaInterface sourceMeta = getInputRowMeta().getValueMeta(data.keynrs[i]);
          data.formatMeta[i] = sourceMeta.clone();
        }

        Properties loadProperties = options.getStreamLoadProp();
        //builder serializer
        data.serializer = DorisRecordSerializer.builder()
                .setType(loadProperties.getProperty(FORMAT_KEY, CSV))
                .setFieldNames(getInputRowMeta().getFieldNames())
                .setFormatMeta(data.formatMeta)
                .setFieldDelimiter(loadProperties.getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT))
                .setLogChannelInterface(log)
                .setDeletable(options.isDeletable())
                .build();
      }

      //serializer data
      streamLoad.writeRecord(meta.getDatabase(), meta.getTable(), data.serializer.serialize(r));
      putRow( getInputRowMeta(), r );
      incrementLinesOutput();

      return true;
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "DorisStreamLoader.Log.ErrorInStep" ), e );
      setErrors( 1 );
      stopAll();
      setOutputDone(); // signal end to receiver(s)
      return false;
    }
  }

  private void closeOutput() throws Exception {
    logDetailed("Closing output...");
    streamLoad.forceFlush();
    streamLoad.close();
    streamLoad = null;
  }

  @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (DorisStreamLoaderMeta) smi;
    data = (DorisStreamLoaderData) sdi;
    logDebug("Initializing step with meta : " + meta.toString());

    if (super.init(smi, sdi)){
      Properties streamHeaders = new Properties();
      String streamLoadProp = meta.getStreamLoadProp();
      if (StringUtils.isNotBlank(streamLoadProp)) {
        String[] keyValues = streamLoadProp.split(";");
        for (String keyValue : keyValues) {
          String[] kv = keyValue.split(":");
          if (kv.length == 2) {
            streamHeaders.put(kv[0], kv[1]);
          }
        }
      }
      options = DorisOptions.builder()
              .withFenodes(meta.getFenodes())
              .withDatabase(meta.getDatabase())
              .withTable(meta.getTable())
              .withUsername(meta.getUsername())
              .withPassword(meta.getPassword())
              .withBufferFlushMaxBytes(meta.getBufferFlushMaxBytes())
              .withBufferFlushMaxRows(meta.getBufferFlushMaxRows())
              .withMaxRetries(meta.getMaxRetries())
              .withStreamLoadProp(streamHeaders)
              .withDeletable(meta.isDeletable()).build();

      logDetailed("Initializing step with options: " + options.toString());
      streamLoad = new DorisBatchStreamLoad(options, log);
      return true;
    }
    return false;
  }

  @Override
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (DorisStreamLoaderMeta) smi;
    data = (DorisStreamLoaderData) sdi;
    // Close the output streams if still needed.
    try {
      if (streamLoad != null && streamLoad.isLoadThreadAlive()) {
        streamLoad.forceFlush();
        streamLoad.close();
        streamLoad = null;
      }
    } catch (Exception e) {
      setErrors(1L);
      logError(BaseMessages.getString(PKG, "DorisStreamLoader.Message.UNEXPECTEDERRORCLOSING"), e);
    }

    super.dispose( smi, sdi );
  }

  @VisibleForTesting
  public DorisBatchStreamLoad getStreamLoad(){
    return streamLoad;
  }
}
