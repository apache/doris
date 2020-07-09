package org.apache.doris.http.interceptor;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import org.apache.doris.http.controller.BaseController;
import org.apache.doris.http.HttpAuthManager;
import org.apache.doris.http.HttpAuthManager.SessionValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

@Component
public class AuthInterceptor extends BaseController implements HandlerInterceptor {
    private Logger logger = LoggerFactory.getLogger(AuthInterceptor.class);
    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response, Object handler) throws Exception {
        String sessionId = getCookieValue(request,BaseController.PALO_SESSION_ID,response);
        SessionValue user = HttpAuthManager.getInstance().getSessionValue(sessionId);
        String method =request.getMethod();
        if(method.equalsIgnoreCase(RequestMethod.OPTIONS.toString())){
            response.setStatus(HttpStatus.NO_CONTENT.value());
            return true;
        } else {
            String authorization = request.getHeader("Authorization");
            if (!Strings.isNullOrEmpty(authorization) && user == null) {
                request.setAttribute("Authorization", authorization);
                if (checkAuthWithCookie(request, response)) {
                    return true;
                } else {
                    Map<String, Object> map = new HashMap<>();
                    map.put("code", 500);
                    map.put("msg", "Authentication Failed.");
                    response.getOutputStream().println(JSON.toJSONString(map));
                    logger.error("Authentication Failed");
                    return false;
                }
            } else if (user == null || user.equals("")) {
                Map<String, Object> map = new HashMap<>();
                map.put("code", 500);
                map.put("msg", "Authentication Failed.");
                response.getOutputStream().println(JSON.toJSONString(map));
                logger.error("Authentication Failed");
                return false;
            }
        }
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest request, HttpServletResponse response, Object handler, ModelAndView modelAndView) throws Exception {
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
    }
}
