package org.apache.doris.http.controller;

import com.alibaba.fastjson.JSON;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;


@RestController
@RequestMapping("/rest/v1")
public class LoginController extends BaseController {

    @RequestMapping(path = "/login",method = RequestMethod.POST)
    public Object login(HttpServletRequest request, HttpServletResponse response, @RequestBody String body){
        Map<String,String> result = (Map<String, String>) JSON.parse(body);
        String auth = result.get("username") + ":" + result.get("password");
        request.setAttribute("Authorization",auth);
        Map<String,Object> msg = new HashMap<>();
        try {
            if(checkAuthWithCookie(request,response)){
                msg.put("code",200);
                msg.put("msg","Login success!");
            } else {
                msg.put("code",401);
                msg.put("msg","Login fail!");
            }
            return msg;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return msg;
    }
}
