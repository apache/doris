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

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.junit.rules.RestorePDIEngineEnvironment;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.ArrayLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.IntLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.LongLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.StringLoadSaveValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisStreamLoaderMetaTest {

    @ClassRule
    public static RestorePDIEngineEnvironment env = new RestorePDIEngineEnvironment();

    @Test
    public void testRoundTrip() throws KettleException {
        List<String> attributes =
            Arrays.asList( "fenodes", "database", "table", "username", "password",
                "streamLoadProp", "bufferFlushMaxRows", "bufferFlushMaxBytes", "maxRetries",
                "deletable",  "stream_name", "field_name");

        Map<String, String> getterMap = new HashMap<>();
        getterMap.put( "fenodes", "getFenodes" );
        getterMap.put( "database", "getDatabase" );
        getterMap.put( "table", "getTable" );
        getterMap.put( "username", "getUsername" );
        getterMap.put( "password", "getPassword" );
        getterMap.put( "streamLoadProp", "getStreamLoadProp" );
        getterMap.put( "bufferFlushMaxRows", "getBufferFlushMaxRows" );
        getterMap.put( "bufferFlushMaxBytes", "getBufferFlushMaxBytes" );
        getterMap.put( "maxRetries", "getMaxRetries" );
        getterMap.put( "deletable", "isDeletable" );
        getterMap.put( "stream_name", "getFieldTable" );
        getterMap.put( "field_name", "getFieldStream" );

        Map<String, String> setterMap = new HashMap<>();
        setterMap.put( "fenodes", "setFenodes" );
        setterMap.put( "database", "setDatabase" );
        setterMap.put( "table", "setTable" );
        setterMap.put( "username", "setUsername" );
        setterMap.put( "password", "setPassword" );
        setterMap.put( "streamLoadProp", "setStreamLoadProp" );
        setterMap.put( "bufferFlushMaxRows", "setBufferFlushMaxRows" );
        setterMap.put( "bufferFlushMaxBytes", "setBufferFlushMaxBytes" );
        setterMap.put( "maxRetries", "setMaxRetries" );
        setterMap.put( "deletable", "setDeletable" );
        setterMap.put( "stream_name", "setFieldTable" );
        setterMap.put( "field_name", "setFieldStream" );

        Map<String, FieldLoadSaveValidator<?>> fieldLoadSaveValidatorAttributeMap = new HashMap<>();
        FieldLoadSaveValidator<String[]> stringArrayLoadSaveValidator = new ArrayLoadSaveValidator<>( new StringLoadSaveValidator(), 25 );

        fieldLoadSaveValidatorAttributeMap.put("maxRetries", new IntLoadSaveValidator( Integer.MAX_VALUE ));
        fieldLoadSaveValidatorAttributeMap.put("bufferFlushMaxRows", new LongLoadSaveValidator());
        fieldLoadSaveValidatorAttributeMap.put("bufferFlushMaxBytes", new LongLoadSaveValidator());
        fieldLoadSaveValidatorAttributeMap.put("streamLoadProp", new StringLoadSaveValidator());
        fieldLoadSaveValidatorAttributeMap.put("stream_name", stringArrayLoadSaveValidator );
        fieldLoadSaveValidatorAttributeMap.put("field_name", stringArrayLoadSaveValidator );

        LoadSaveTester loadSaveTester =
            new LoadSaveTester( DorisStreamLoaderMeta.class, attributes, getterMap, setterMap,
                fieldLoadSaveValidatorAttributeMap, new HashMap<String, FieldLoadSaveValidator<?>>() );

        loadSaveTester.testSerialization();
    }

    @Test
    public void testPDI16559() throws Exception {
        DorisStreamLoaderMeta streamLoader = new DorisStreamLoaderMeta();
        streamLoader.setFieldTable( new String[] { "table1", "table2", "table3" } );
        streamLoader.setFieldStream( new String[] { "stream1" } );
        streamLoader.setTable( "test_table" );

        try {
            String badXml = streamLoader.getXML();
            Assert.fail( "Before calling afterInjectionSynchronization, should have thrown an ArrayIndexOOB" );
        } catch ( Exception expected ) {
            // Do Nothing
        }
        streamLoader.afterInjectionSynchronization();
        //run without a exception
        String ktrXml = streamLoader.getXML();

        int targetSz = streamLoader.getFieldTable().length;
        Assert.assertEquals( targetSz, streamLoader.getFieldStream().length );
    }
}
