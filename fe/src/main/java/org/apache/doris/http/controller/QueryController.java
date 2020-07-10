package org.apache.doris.http.controller;

import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class QueryController {

    private static final Logger LOG = LogManager.getLogger(QueryController.class);

    @RequestMapping(path = "/query",method = RequestMethod.GET)
    public Object query(){
        List<Map<String,String>> result = new ArrayList<>();
        addFinishedQueryInfo(result);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(result);
        entity.setCount(result.size());
        return entity;
    }

    // Note: we do not show 'Query ID' column in web page
    private void addFinishedQueryInfo(List<Map<String,String>> result) {
        List<List<String>> finishedQueries = ProfileManager.getInstance().getAllQueries();
        List<String> columnHeaders = ProfileManager.PROFILE_HEADERS;
        int queryIdIndex = 0; // the first column is 'Query ID' by default
        for (int i = 0; i < columnHeaders.size(); ++i) {
            if (columnHeaders.get(i).equals(ProfileManager.QUERY_ID)) {
                queryIdIndex = i;
            }
        }
        appendFinishedQueryTableBody( result, finishedQueries, columnHeaders, queryIdIndex);
    }

    private void appendFinishedQueryTableBody( List<Map<String,String>> result, List<List<String>> bodies, List<String> columnHeaders,
                                               int queryIdIndex) {

        for ( List<String> row : bodies) {
            String queryId = row.get(queryIdIndex);
            Map<String,String> rowMap = new HashMap<>();
            for (int i = 0; i < row.size(); ++i) {
                if (i == queryIdIndex) {
                    continue;
                }
                rowMap.put(columnHeaders.get(i),row.get(i));
            }
            // add 'Profile' column
            if (Strings.isNullOrEmpty(queryId)) {
                LOG.warn("query id is null or empty, maybe we forget to push it "
                        + "into array when generate profile info.");
                rowMap.put("queryId","Empty Query ID");
            } else {
                rowMap.put("QueryId",queryId);
            }
            result.add(rowMap);
        }
    }
}
