package org.apache.doris.http.controller;


import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.collect.Lists;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/rest/v1")
public class SessionController {

    private static final ArrayList<String> SESSION_TABLE_HEADER = Lists.newArrayList();

    static {
        SESSION_TABLE_HEADER.add("Id");
        SESSION_TABLE_HEADER.add("User");
        SESSION_TABLE_HEADER.add("Host");
        SESSION_TABLE_HEADER.add("Cluster");
        SESSION_TABLE_HEADER.add("Db");
        SESSION_TABLE_HEADER.add("Command");
        SESSION_TABLE_HEADER.add("Time");
        SESSION_TABLE_HEADER.add("State");
        SESSION_TABLE_HEADER.add("Info");
    }

    @RequestMapping(path = "/session",method = RequestMethod.GET)
    public Object session(){
        List<Map<String,String>> result = new ArrayList<>();
        appendSessionInfo(result);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(result);
        return entity;
    }


    private void appendSessionInfo(List<Map<String,String>> result) {
        List<ConnectContext.ThreadInfo> threadInfos = ExecuteEnv.getInstance().getScheduler().listConnection("root");
        List<List<String>> rowSet = Lists.newArrayList();
        long nowMs = System.currentTimeMillis();
        for (ConnectContext.ThreadInfo info : threadInfos) {
            rowSet.add(info.toRow(nowMs));
        }

        for (List<String> row : rowSet){
            Map<String,String> record = new HashMap<>();
            for(int i = 0; i < row.size(); i ++){
                record.put(SESSION_TABLE_HEADER.get(i),row.get(i));
            }
            result.add(record);
        }
    }
}
