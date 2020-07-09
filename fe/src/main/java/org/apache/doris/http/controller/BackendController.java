package org.apache.doris.http.controller;



import java.util.*;

import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.system.Backend;
import com.google.common.collect.ImmutableMap;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/rest/v1")
public class BackendController {

    private static final Logger LOG = LogManager.getLogger(BackendController.class);


    @RequestMapping(path = "/backend",method = RequestMethod.GET)
    public Object backend(){
        ImmutableMap<Long, Backend> backendMap = Catalog.getCurrentSystemInfo().getIdToBackend();
        Map<String,Backend> result = new HashMap<>();
        for(long key : backendMap.keySet()){
            result.put(Long.toString(key),backendMap.get(key));
        }
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(result);
        return entity;
    }
}
