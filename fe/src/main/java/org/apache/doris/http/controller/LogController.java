package org.apache.doris.http.controller;

import org.apache.doris.common.Config;
import org.apache.doris.common.Log4jConfig;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.commons.lang.StringUtils;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class LogController {

    private static final Logger LOG = LogManager.getLogger(LogController.class);
    private static long WEB_LOG_BYTES = 1024 * 1024;  // 1MB

    private String addVerboseName;
    private String delVerboseName;

    @RequestMapping(path = "/log",method = RequestMethod.GET)
    public Object log(HttpServletRequest request){
        Map<String,Map<String,String>> map = new HashMap<>();
        // get parameters
        addVerboseName = request.getParameter("add_verbose");
        delVerboseName = request.getParameter("del_verbose");
        LOG.info("add verbose name: {}, del verbose name: {}", addVerboseName, delVerboseName);

        appendLogConf(map);
        appendLogInfo(map);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(map);
        return entity;
    }

    private void appendLogConf(Map<String,Map<String,String>> content) {
        Map<String,String> map = new HashMap<>();

        try {
            Log4jConfig.Tuple<String, String[], String[]> configs = Log4jConfig.updateLogging(null, null, null);
            if (!Strings.isNullOrEmpty(addVerboseName)) {
                addVerboseName = addVerboseName.trim();
                List<String> verboseNames = Lists.newArrayList(configs.y);
                if (!verboseNames.contains(addVerboseName)) {
                    verboseNames.add(addVerboseName);
                    configs = Log4jConfig.updateLogging(null, verboseNames.toArray(new String[verboseNames.size()]),
                            null);
                }
            }
            if (!Strings.isNullOrEmpty(delVerboseName)) {
                delVerboseName = delVerboseName.trim();
                List<String> verboseNames = Lists.newArrayList(configs.y);
                if (verboseNames.contains(delVerboseName)) {
                    verboseNames.remove(delVerboseName);
                    configs = Log4jConfig.updateLogging(null, verboseNames.toArray(new String[verboseNames.size()]),
                            null);
                }
            }

            map.put("Level", configs.x);
            map.put("VerboseNames" , StringUtils.join(configs.y, ",") );
            map.put("AuditNames",StringUtils.join(configs.z, ","));
            content.put("LogConfiguration",map);
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }
    }

    private void appendLogInfo(Map<String,Map<String,String>> content) {
        Map<String,String> map = new HashMap<>();

        final String logPath = Config.sys_log_dir + "/fe.warn.log";
        map.put("logPath",logPath );

        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(logPath, "r");
            long fileSize = raf.length();
            long startPos = fileSize < WEB_LOG_BYTES ? 0L : fileSize - WEB_LOG_BYTES;
            long webContentLength = fileSize < WEB_LOG_BYTES ? fileSize : WEB_LOG_BYTES;
            raf.seek(startPos);
            map.put("Showinglast" , webContentLength + " bytes of log");
            StringBuffer buffer = new StringBuffer();
            String fileBuffer = null;
            buffer.append("<pre>");
            while ((fileBuffer = raf.readLine()) != null) {
                buffer.append(fileBuffer).append("</br>");
            }
            buffer.append("</pre>");
            map.put("log",buffer.toString());

        } catch (FileNotFoundException e) {
            map.put("error","Couldn't open log file: "+ logPath);
        } catch (IOException e) {
            map.put("error","Failed to read log file: " + logPath );
        } finally {
            try {
                if (raf != null) {
                    raf.close();
                }
            } catch (IOException e) {
                LOG.warn("fail to close log file: " + logPath, e);
            }
        }
        content.put("LogContents",map);
    }
}
