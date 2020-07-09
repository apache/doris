package org.apache.doris.http.controller;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Strings;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.proc.ProcDirInterface;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.ShowResultSet;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/rest/v1")
public class SystemController {

    private static final Logger LOG = LogManager.getLogger(SystemController.class);


    @RequestMapping(path = "/system",method = RequestMethod.GET)
    public Object system(HttpServletRequest request){
        String currentPath = request.getParameter("path");
        if (Strings.isNullOrEmpty(currentPath)) {
            currentPath = "/";
        }
        ResponseEntity entity = appendSystemInfo(currentPath, currentPath);
        return entity;
    }


    protected ProcNodeInterface getProcNode(String path) {
        ProcService instance = ProcService.getInstance();
        ProcNodeInterface node;
        try {
            if (Strings.isNullOrEmpty(path)) {
                node = instance.open("/");
            } else {
                node = instance.open(path);
            }
        } catch (AnalysisException e) {
            LOG.warn(e.getMessage());
            return null;
        }
        return node;
    }


    private ResponseEntity appendSystemInfo(String procPath, String path) {


        List<Map<String,String>> list = new ArrayList<>();
        Map<String,Object> map = new HashMap<>();

        ProcNodeInterface procNode = getProcNode(procPath);
        if (procNode == null) {
            map.put("code","404");
            map.put("msg","No such proc path[" + path + "]");
            ResponseEntity entity = ResponseEntity.status(HttpStatus.NOT_FOUND).build(map);
            return entity;
        }
        boolean isDir = (procNode instanceof ProcDirInterface);

        List<String> columnNames = null;
        List<List<String>> rows = null;
        if (!Catalog.getCurrentCatalog().isMaster()) {
            // forward to master
            String showProcStmt = "SHOW PROC \"" + procPath + "\"";
            MasterOpExecutor masterOpExecutor = new MasterOpExecutor(new OriginStatement(showProcStmt, 0),
                    ConnectContext.get(), RedirectStatus.FORWARD_NO_SYNC);
            try {
                masterOpExecutor.execute();
            } catch (Exception e) {
                LOG.warn("Fail to forward. ", e);
                map.put("code","500");
                map.put("msg","Failed to forward request to master");
                ResponseEntity entity = ResponseEntity.status(HttpStatus.SERVICE_ERROR).build(map);
                return entity;
            }

            ShowResultSet resultSet = masterOpExecutor.getProxyResultSet();
            if (resultSet == null) {
                map.put("code","505");
                map.put("msg","Failed to get result from master");
                ResponseEntity entity = ResponseEntity.status(HttpStatus.SERVICE_ERROR).build(map);
                return entity;
            }

            columnNames = resultSet.getMetaData().getColumns().stream().map(c -> c.getName()).collect(
                    Collectors.toList());
            rows = resultSet.getResultRows();
        } else {
            ProcResult result;
            try {
                result = procNode.fetchResult();
            } catch (AnalysisException e) {
                String meg = "The result is null,maybe haven't be implemented completely[" + e.getMessage() + "], please check."
                        + "INFO: ProcNode type is [" + procNode.getClass().getName() + "]";
                map.put("msg",meg);
                map.put("code",500);
                ResponseEntity entity = ResponseEntity.status(HttpStatus.SERVICE_ERROR).build(map);
                return entity;
            }

            columnNames = result.getColumnNames();
            rows = result.getRows();
        }

        Preconditions.checkNotNull(columnNames);
        Preconditions.checkNotNull(rows);



        for ( List<String> strList : rows) {
            Map<String,String> resultMap = new HashMap<>();

            int columnIndex = 1;
            for (int i = 0; i < strList.size() ; i++) {
                if (isDir && columnIndex == 1) {
                    String str = strList.get(i);
                    resultMap.put(columnNames.get(0),str);
                    String escapeStr = str.replace("%", "%25");
                    String uriPath = "path=" +  path + "/" + escapeStr;
                    resultMap.put("hrefPath",uriPath);
                } else {
                    resultMap.put(columnNames.get(i), strList.get(i));
                }
                ++columnIndex;
            }
            list.add(resultMap);
        }
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(list);
        return entity;
    }

    // some expamle:
    //   '/'            => '/'
    //   '///aaa'       => '///'
    //   '/aaa/bbb///'  => '/aaa'
    //   '/aaa/bbb/ccc' => '/aaa/bbb'
    // ATTN: the root path's parent is itself.
    private String getParentPath(String path) {
        int lastSlashIndex = path.length() - 1;
        while (lastSlashIndex > 0) {
            int tempIndex = path.lastIndexOf('/', lastSlashIndex);
            if (tempIndex > 0) {
                if (tempIndex == lastSlashIndex) {
                    lastSlashIndex = tempIndex - 1;
                    continue;
                } else if (tempIndex < lastSlashIndex) { // '//aaa/bbb'
                    lastSlashIndex = tempIndex;
                    return path.substring(0, lastSlashIndex);
                }
            }
        }
        return "/";
    }
}
