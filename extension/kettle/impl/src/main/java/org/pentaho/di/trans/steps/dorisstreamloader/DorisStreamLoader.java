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

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisBatchStreamLoad;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisDataType;
import org.pentaho.di.trans.steps.dorisstreamloader.load.DorisOptions;
import org.pentaho.di.trans.steps.dorisstreamloader.serializer.DorisRecordSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.pentaho.di.trans.steps.dorisstreamloader.load.LoadConstants.*;

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

    public Object[] transform(Object[] r, boolean supportUpsertDelete) throws KettleException {
        Object[] values = new Object[data.keynrs.length + (supportUpsertDelete ? 1 : 0)];
        for (int i = 0; i < data.keynrs.length; i++) {
            ValueMetaInterface sourceMeta = getInputRowMeta().getValueMeta(data.keynrs[i]);
            DorisDataType dataType = data.fieldtype.get(meta.getFieldTable()[i]);
            values[i] = typeConversion(sourceMeta, dataType, r[i]);
        }
        return values;
    }

    public Object typeConversion(ValueMetaInterface sourceMeta, DorisDataType type, Object r) throws KettleException {
        if (r == null) {
            return null;
        }
        try {
            switch (sourceMeta.getType()) {
                case ValueMetaInterface.TYPE_STRING:
                    String sValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        sValue = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        sValue = sourceMeta.getString(r);
                    }
                    return sValue;
                case ValueMetaInterface.TYPE_BOOLEAN:
                    Boolean boolenaValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        String binaryBoolean = new String((byte[]) r, StandardCharsets.UTF_8);
                        boolenaValue = binaryBoolean.equals("1") || binaryBoolean.equals("true") || binaryBoolean.equals("True") || binaryBoolean.equals("TRUE");
                    } else {
                        boolenaValue = sourceMeta.getBoolean(r);
                    }
                    return boolenaValue;
                case ValueMetaInterface.TYPE_INTEGER:
                    Long integerValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        integerValue = Long.parseLong(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        integerValue = sourceMeta.getInteger(r);
                    }
                    if (integerValue >= Byte.MIN_VALUE && integerValue <= Byte.MAX_VALUE && type == DorisDataType.TINYINT) {
                        return integerValue.byteValue();
                    } else if (integerValue >= Short.MIN_VALUE && integerValue <= Short.MAX_VALUE && type == DorisDataType.SMALLINT) {
                        return integerValue.shortValue();
                    } else if (integerValue >= Integer.MIN_VALUE && integerValue <= Integer.MAX_VALUE && type == DorisDataType.INT) {
                        return integerValue.intValue();
                    } else {
                        return integerValue;
                    }
                case ValueMetaInterface.TYPE_NUMBER:
                    Double doubleValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        doubleValue = Double.parseDouble(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        doubleValue = sourceMeta.getNumber(r);
                    }
                    return doubleValue;
                case ValueMetaInterface.TYPE_BIGNUMBER:
                    BigDecimal decimalValue;
                    if (sourceMeta.isStorageBinaryString()) {
                        decimalValue = new BigDecimal(new String((byte[]) r, StandardCharsets.UTF_8));
                    } else {
                        decimalValue = sourceMeta.getBigNumber(r);
                    }
                    return decimalValue; // BigDecimal string representation is compatible with DECIMAL
                case ValueMetaInterface.TYPE_DATE:
                    SimpleDateFormat sourceDateFormatter = sourceMeta.getDateFormat();
                    SimpleDateFormat dateFormatter = null;
                    if (type == DorisDataType.DATE) {
                        // StarRocks DATE type format: 'yyyy-MM-dd'
                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    } else {
                        // StarRocks DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
                        dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    Date dateValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        String dateStr = new String((byte[]) r, StandardCharsets.UTF_8);
                        dateValue = sourceDateFormatter.parse(dateStr);
                    } else {
                        dateValue = sourceMeta.getDate(r);
                    }

                    return dateFormatter.format(dateValue);
                case ValueMetaInterface.TYPE_TIMESTAMP:
                    SimpleDateFormat sourceTimestampFormatter = sourceMeta.getDateFormat();
                    SimpleDateFormat timeStampFormatter = null;
                    if (type == DorisDataType.DATE) {
                        // StarRocks DATE type format: 'yyyy-MM-dd'
                        timeStampFormatter = new SimpleDateFormat("yyyy-MM-dd");
                    } else {
                        // StarRocks DATETIME type format: 'yyyy-MM-dd HH:mm:ss'
                        timeStampFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }
                    Timestamp timestampValue = null;
                    if (sourceMeta.isStorageBinaryString()) {
                        String timestampStr = new String((byte[]) r, StandardCharsets.UTF_8);
                        timestampValue = new Timestamp(sourceTimestampFormatter.parse(timestampStr).getTime());
                    } else {
                        timestampValue = (Timestamp) sourceMeta.getDate(r);
                    }
                    return timeStampFormatter.format(timestampValue);
                case ValueMetaInterface.TYPE_BINARY:
                    throw new KettleException((BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnSupportBinary") + r.toString()));

                case ValueMetaInterface.TYPE_INET:
                    String address;
                    if (sourceMeta.isStorageBinaryString()) {

                        address = new String((byte[]) r, StandardCharsets.UTF_8);
                    } else {
                        address = (String) r;
                    }
                    return address;
                default:
                    throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.UnknowType") + ValueMetaInterface.getTypeDescription(sourceMeta.getType()));
            }
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG, "StarRocksKettleConnector.Message.FailConvertType") + e.getMessage());
        }
    }


    @Override
  public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
    meta = (DorisStreamLoaderMeta) smi;
    data = (DorisStreamLoaderData) sdi;
    if (super.init(smi, sdi)){
      Properties streamHeaders = new Properties();
      String streamLoadProp = meta.getStreamLoadProp().toString();
      if (StringUtils.isNotBlank(streamLoadProp)) {
          if (streamLoadProp.startsWith("{")){
              streamLoadProp= streamLoadProp.substring(1,streamLoadProp.length()-1);
          }
          if (streamLoadProp.endsWith("}")){
              streamLoadProp= streamLoadProp.substring(0,streamLoadProp.length()-2);
          }
        String[] keyValues = streamLoadProp.split(",");
        for (String keyValue : keyValues) {
          String[] kv = keyValue.split(",");
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
              .withStreamLoadProp(streamHeaders).build();
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
}
