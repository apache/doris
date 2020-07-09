package org.apache.doris.http.controller;

import org.apache.doris.common.util.ProfileManager;
import com.google.common.base.Strings;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/rest/v1")
public class QueryProfileController {

    @RequestMapping(path = "/query_profile/{queryId}",method = RequestMethod.GET)
    public Object profile(@PathVariable String queryId){
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build();

        if (Strings.isNullOrEmpty(queryId)) {
            entity.setCode(HttpStatus.BAD_REQUEST.value());
            entity.setMsg("Must specify a query_id[]");
            return entity;
        }
        String profile = ProfileManager.getInstance().getProfile(queryId);
        profile = profile.replaceAll("\n","</br>");
        profile = profile.replaceAll(" ","&nbsp;&nbsp;");
        entity.setData(profile);
        return entity;
    }
}
