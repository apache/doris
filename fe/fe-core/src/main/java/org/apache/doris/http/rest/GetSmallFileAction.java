package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class GetSmallFileAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(GetSmallFileAction.class);


    @RequestMapping(path = "/api/get_small_file",method = RequestMethod.GET)
    public Object execute(HttpServletRequest request, HttpServletResponse response) {
        String token = request.getParameter("token");
        String fileIdStr = request.getParameter("file_id");
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");

        // check param empty
        if (Strings.isNullOrEmpty(token) || Strings.isNullOrEmpty(fileIdStr)) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("Missing parameter");
            return entity;
        }

        // check token
        if (!token.equals(Catalog.getCurrentCatalog().getToken())) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("Invalid token");
            return entity;
        }

        long fileId = -1;
        try {
            fileId = Long.valueOf(fileIdStr);
        } catch (NumberFormatException e) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("Invalid file id format: " + fileIdStr);
            return entity;
        }

        SmallFileMgr fileMgr = Catalog.getCurrentCatalog().getSmallFileMgr();
        SmallFileMgr.SmallFile smallFile = fileMgr.getSmallFile(fileId);
        if (smallFile == null || !smallFile.isContent) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("File not found or is not content");
            return entity;
        }

        String method = request.getMethod();
        if (method.equalsIgnoreCase("GET")) {
            boolean isSuccess = getFile(request,response,smallFile.getContentBytes(),smallFile.name);
            if(isSuccess) {
                return entity;
            } else {
                entity.setCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
                entity.setMsg(HttpStatus.INTERNAL_SERVER_ERROR.name());
                return entity;
            }
        } else {
            entity.setCode(HttpStatus.METHOD_NOT_ALLOWED.value());
            entity.setMsg("HTTP method is not allowed");
            return entity;
        }
    }
}
