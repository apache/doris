// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.http.action;

import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.common.proc.ProcNodeInterface;
import com.baidu.palo.common.proc.ProcService;
import com.baidu.palo.http.ActionController;
import com.baidu.palo.http.BaseAction;
import com.baidu.palo.http.BaseRequest;
import com.baidu.palo.http.BaseResponse;
import com.baidu.palo.http.HttpAuthManager;
import com.baidu.palo.http.rest.RestBaseResult;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.handler.codec.http.DefaultCookie;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

public class WebBaseAction extends BaseAction {
    private static final Logger LOG = LogManager.getLogger(WebBaseAction.class);
    private static final String ADMIN_USER = "root";

    protected static final String LINE_SEP = System.getProperty("line.separator");

    protected static final String PALO_SESSION_ID = "PALO_SESSION_ID";
    private static final long PALO_SESSION_EXPIRED_TIME = 3600 * 24; // one day

    protected static final String PAGE_HEADER = "<!DOCTYPE html>"
            + "<html>"
            + "<head>"
            + "  <title>Baidu Palo</title>"
            + "  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" >"

            + "  <link href=\"static/css?res=bootstrap.css\" "
            + "  rel=\"stylesheet\" media=\"screen\"/>"
            + "  <link href=\"static/css?res=datatables_bootstrap.css\" "
            + "    rel=\"stylesheet\" media=\"screen\"/>"

            + "  <script type=\"text/javascript\" src=\"static?res=jquery.js\"></script>"
            + "  <script type=\"text/javascript\" src=\"static?res=jquery.dataTables.js\"></script>"
            + "  <script type=\"text/javascript\" src=\"static?res=datatables_bootstrap.js\"></script>" + LINE_SEP

            + "  <script type=\"text/javascript\"> " + LINE_SEP
            + "    $(document).ready(function() { " + LINE_SEP
            + "      $('#table_id').dataTable({ " + LINE_SEP
            + "        \"sScrollX\": \"100%\"," + LINE_SEP
            + "        \"aaSorting\": []," + LINE_SEP
            +       " });" + LINE_SEP
            + "    }); " + LINE_SEP
            + "  </script> " + LINE_SEP

            + "  <style>"
            + "    body {"
            + "      padding-top: 60px;"
            + "    }"
            + "  </style>"
            + "</head>"
            + "<body>";
    protected static final String PAGE_FOOTER = "</div></body></html>";
    protected static final String NAVIGATION_BAR_PREFIX =
              "  <div class=\"navbar navbar-inverse navbar-fixed-top\">"
            + "    <div class=\"navbar-inner\">"
            + "      <div class=\"container\">"
            + "        <a class=\"btn btn-navbar\" data-toggle=\"collapse\" "
            + "            data-target=\".nav-collapse\">"
            + "          <span class=\"icon-bar\"></span>"
            + "          <span class=\"icon-bar\"></span>"
            + "          <span class=\"icon-bar\"></span>"
            + "        </a>"
            + "        <a class=\"brand\" href=\"/\">Palo</a>"
            + "        <div class=\"nav-collapse collapse\">"
            + "          <ul class=\"nav\">";
    protected static final String NAVIGATION_BAR_SUFFIX =
              "          </ul>"
            + "        </div>"
            + "      </div>"
            + "    </div>"
            + "  </div>"
            + "  <div class=\"container\">";

    public WebBaseAction(ActionController controller) {
        super(controller);
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        if (!checkAuth(request, response)) {
            return;
        }

        HttpMethod method = request.getRequest().method();
        if (method.equals(HttpMethod.GET)) {
            executeGet(request, response);
        } else if (method.equals(HttpMethod.POST)) {
            executePost(request, response);
        } else {
            response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
            writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    // Sub Action class should overvide this method
    public void executeGet(BaseRequest request, BaseResponse response) {
        response.appendContent(new RestBaseResult("Not implemented").toJson());
        writeResponse(request, response, HttpResponseStatus.NOT_IMPLEMENTED);
    }

    // Sub Action class should overvide this method
    public void executePost(BaseRequest request, BaseResponse response) {
        response.appendContent(new RestBaseResult("Not implemented").toJson());
        writeResponse(request, response, HttpResponseStatus.NOT_IMPLEMENTED);
    }

    // We first check cookie, if not admin, we check http's authority header
    protected boolean checkAuth(BaseRequest request, BaseResponse response) {
        if (checkAuthByCookie(request, response)) {
            return true;
        }

        if (needAdmin()) {
            try {
                checkAdmin(request);
                request.setAdmin(true);
                addSession(request, response, ADMIN_USER);
                return true;
            } catch (DdlException e) {
                response.appendContent("Authentication Failed. <br/> "
                        + "You can only access <a href=\"/help\">'/help'</a> page without login!");
                writeAuthResponse(request, response);
                return false;
            }
        }

        return true;
    }

    protected boolean checkAuthByCookie(BaseRequest request, BaseResponse response) {
        String sessionId = request.getCookieValue(PALO_SESSION_ID);
        HttpAuthManager authMgr = HttpAuthManager.getInstance();
        String username = "";
        if (!Strings.isNullOrEmpty(sessionId)) {
            username = authMgr.getUsername(sessionId);
            if (!Strings.isNullOrEmpty(username)) {
                if (username.equals(ADMIN_USER)) {
                    response.updateCookieAge(request, PALO_SESSION_ID, PALO_SESSION_EXPIRED_TIME);
                    request.setAdmin(true);
                    return true;
                }
            }
        }
        return false;
    }

    // ATTN: sub Action classes can override it when there is no need to check authority.
    //       eg. It is no need admin privileges to access to HelpAction, so we will override this
    //       mothod in HelpAction by returning false.
    public boolean needAdmin() {
        return true;
    }

    protected void writeAuthResponse(BaseRequest request, BaseResponse response) {
        response.addHeader(HttpHeaders.Names.WWW_AUTHENTICATE, "Basic realm=\"\"");
        writeResponse(request, response, HttpResponseStatus.UNAUTHORIZED);
    }

    protected void writeResponse(BaseRequest request, BaseResponse response) {
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    protected void addSession(BaseRequest request, BaseResponse response, String value) {
        // We use hashcode of client's IP and timestamp, which not only can identify users from
        // different host machine, but also can improve the difficulty of forging cookie.
        int clientAddrHashCode = ((InetSocketAddress) request.getContext().channel().remoteAddress()).hashCode();
        String key = UUID.randomUUID().toString();
        DefaultCookie cookie = new DefaultCookie(PALO_SESSION_ID, key);
        cookie.setMaxAge(PALO_SESSION_EXPIRED_TIME);
        response.addCookie(cookie);
        HttpAuthManager.getInstance().addClient(key, value);
    }

    protected void getPageHeader(BaseRequest request, StringBuilder sb) {
        String newPageHeaderString = PAGE_HEADER;
        newPageHeaderString = newPageHeaderString.replaceAll("<title>Baidu Palo</title>",
                                                             "<title>" + Config.cluster_name + "</title>");

        sb.append(newPageHeaderString);
        sb.append(NAVIGATION_BAR_PREFIX);

        // TODO(lingbin): maybe should change to register the menu item?
        if (request.isAdmin()) {
            sb.append("<li><a href=\"/system\">")
                    .append("system")
                    .append("</a></li>");
            sb.append("<li><a href=\"/backend\">")
                    .append("backends")
                    .append("</a></li>");
            sb.append("<li><a href=\"/log\">")
                    .append("logs")
                    .append("</a></li>");
            sb.append("<li><a href=\"/query\">")
                    .append("queries")
                    .append("</a></li>");
            sb.append("<li><a href=\"/session\">")
                    .append("sessions")
                    .append("</a></li>");
            sb.append("<li><a href=\"/variable\">")
                    .append("variables")
                    .append("</a></li>");
            sb.append("<li><a href=\"/ha\">")
                    .append("ha")
                    .append("</a></li>");
        }
        sb.append("<li><a href=\"/help\">")
                .append("help")
                .append("</a></li>");

        sb.append(NAVIGATION_BAR_SUFFIX);
    }

    protected void getPageFooter(StringBuilder sb) {
        sb.append(PAGE_FOOTER);
    }

    // Note: DO NOT omit '<thead>', because it is useful in 'datatable' plugin.
    protected void appendTableHeader(StringBuilder buffer, List<String> columnHeaders) {
        buffer.append("<table id=\"table_id\" "
                + "class=\"table table-hover table-bordered table-striped table-condensed\">");
        buffer.append("<thead><tr> ");
        for (String str : columnHeaders) {
            buffer.append("<th>" + str + "</th>");
        }
        buffer.append(" </tr></thead>");
    }

    protected void appendTableBody(StringBuilder buffer, List<List<String>> bodies) {
        buffer.append("<tbody>");
        for ( List<String> row : bodies) {
            buffer.append("<tr>");
            for (String column : row) {
                buffer.append("<td>");
                buffer.append(column);
                buffer.append("</td>");
            }
            buffer.append("</tr>");
        }
        buffer.append("</tbody>");
    }

    protected void appendTableFooter(StringBuilder buffer) {
        buffer.append("</table>");
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

    // Because org.apache.commons.lang.StringEscapeUtils.escapeHtml() not only escape tags in html,
    // but also escppe Chinese character code, which may cause Chinese garbled in browser, so we
    // define our own simplified escape method here.
    // ATTN: we should make sure file-encoding of help files is utf-8
    protected String escapeHtmlInPreTag(String oriStr) {
        if (oriStr == null) {
            return "";
        }

        StringBuilder buff = new StringBuilder();
        char[] chars = oriStr.toCharArray();
        for (int i = 0; i < chars.length; ++i) {
            switch (chars[i]) {
                case '<':
                    buff.append("&lt;");
                    break;
                case '>':
                    buff.append("&lt;");
                    break;
                case '"':
                    buff.append("&quot;");
                    break;
                case '&':
                    buff.append("&amp;");
                    break;
                default:
                    buff.append(chars[i]);
                    break;
            }
        }

        return buff.toString();
    }

    private static final NotFoundAction NOT_FOUND_ACTION = new NotFoundAction(null);

    public static NotFoundAction getNotFoundAction() {
        return NOT_FOUND_ACTION;
    }
}
