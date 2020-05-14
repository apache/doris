// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.http.action;

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcService;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseAction;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.HttpAuthManager;
import org.apache.doris.http.HttpAuthManager.SessionValue;
import org.apache.doris.http.UnauthorizedException;
import org.apache.doris.http.rest.RestBaseResult;
import org.apache.doris.mysql.privilege.PaloPrivilege;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.cookie.DefaultCookie;

public class WebBaseAction extends BaseAction {
    private static final Logger LOG = LogManager.getLogger(WebBaseAction.class);

    protected static final String PALO_SESSION_ID = "PALO_SESSION_ID";
    private static final long PALO_SESSION_EXPIRED_TIME = 3600 * 24; // one day

    protected static final String PAGE_HEADER = "<!DOCTYPE html>"
            + "<html>"
            + "<head>"
            + "  <title>Apache Doris(Incubating)</title>"
            + "  <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" >"
            + "  <link rel=\"shortcut icon\" href=\"/static/images?res=favicon.ico\">"
            + "  <link href=\"/static/css?res=bootstrap.css\" "
            + "  rel=\"stylesheet\" media=\"screen\"/>"
            + "  <link href=\"/static/css?res=bootstrap-theme.css\" "
            + "  rel=\"stylesheet\" media=\"screen\"/>"
            + "  <link href=\"/static/css?res=datatables_bootstrap.css\" "
            + "    rel=\"stylesheet\" media=\"screen\"/>"

            + "  <script type=\"text/javascript\" src=\"/static?res=jquery.js\"></script>"
            + "  <script type=\"text/javascript\" src=\"/static?res=jquery.dataTables.js\"></script>"
            + "  <script type=\"text/javascript\" src=\"/static?res=datatables_bootstrap.js\"></script>"

            + "  <script type=\"text/javascript\"> "
            + "    $(document).ready(function() { "
            + "      $('#table_id').dataTable({ "
            + "        \"aaSorting\": [],"
            + "        \"lengthMenu\": [[10, 25, 50, 100,-1], [10, 25, 50, 100, \"All\"]],"
            + "        \"iDisplayLength\": 50,"
            +       " });"
            + "    }); "
            + "    $(document).ready(function () {"
            + "        var location = window.location.pathname;"
            + "        var id = location.substring(location.lastIndexOf('/') + 1);"
            + "        if (id != null || $.trim(id) != \"\") {"
            + "           $(\"#nav_\" + id).addClass(\"active\");"
            + "        }"
            + "    });"
            + "  </script> "

            + "  <style>"
            + "    body {"
            + "      padding-top: 60px;"
            + "    }"
            + "  </style>"
            + "</head>"
            + "<body>";
    protected static final String PAGE_FOOTER = "</div></body></html>";
    protected static final String NAVIGATION_BAR_PREFIX =
              "  <nav class=\"navbar navbar-default navbar-fixed-top\" role=\"navigation\">"
            + "    <div class=\"container-fluid\">"
            + "    <div class=\"navbar-header\">"
            + "      <a class=\"navbar-brand\" href=\"/\" style=\"padding: unset;\">"
            + "        <img alt=\"Doris\" style=\"height: inherit;\" src=\"/static/images?res=doris-logo.png\">"
            + "      </a>"
            + "    </div>"
            + "    <div>"
            + "      <ul class=\"nav navbar-nav\" role=\"tablist\">";
    protected static final String NAVIGATION_BAR_SUFFIX =
              "      </ul>"
            + "    </div>"
            + "  </nav>"
            + "  <div class=\"container\">";

    public WebBaseAction(ActionController controller) {
        super(controller);
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        if (!checkAuthWithCookie(request, response)) {
            return;
        }

        HttpMethod method = request.getRequest().method();
        if (method.equals(HttpMethod.GET)) {
            executeGet(request, response);
        } else if (method.equals(HttpMethod.POST)) {
            executePost(request, response);
        } else {
            response.appendContent(new RestBaseResult("HTTP method is not allowed: " + method.name()).toJson());
            writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }

    // Sub Action class should override this method
    public void executeGet(BaseRequest request, BaseResponse response) {
        response.appendContent(new RestBaseResult("Not implemented").toJson());
        writeResponse(request, response, HttpResponseStatus.NOT_IMPLEMENTED);
    }

    // Sub Action class should override this method
    public void executePost(BaseRequest request, BaseResponse response) {
        response.appendContent(new RestBaseResult("Not implemented").toJson());
        writeResponse(request, response, HttpResponseStatus.NOT_IMPLEMENTED);
    }

    // We first check cookie, if not admin, we check http's authority header
    private boolean checkAuthWithCookie(BaseRequest request, BaseResponse response) {
        if (!needPassword()) {
            return true;
        }

        if (checkCookie(request, response)) {
            return true;
        }

        // cookie is invalid.
        ActionAuthorizationInfo authInfo;
        try {
            authInfo = getAuthorizationInfo(request);
            UserIdentity currentUser = checkPassword(authInfo);
            if (needAdmin()) {
                checkGlobalAuth(currentUser, PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                        PaloPrivilege.NODE_PRIV), Operator.OR));
            }
            request.setAuthorized(true);
            SessionValue value = new SessionValue();
            value.currentUser = currentUser;
            addSession(request, response, value);

            ConnectContext ctx = new ConnectContext(null);
            ctx.setQualifiedUser(authInfo.fullUserName);
            ctx.setRemoteIP(authInfo.remoteIp);
            ctx.setCurrentUserIdentity(currentUser);
            ctx.setCatalog(Catalog.getCurrentCatalog());
            ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
            ctx.setThreadLocalInfo();

            return true;
        } catch (UnauthorizedException e) {
            response.appendContent("Authentication Failed. <br/> " + e.getMessage());
            writeAuthResponse(request, response);
            return false;
        }
    }

    private boolean checkCookie(BaseRequest request, BaseResponse response) {
        String sessionId = request.getCookieValue(PALO_SESSION_ID);
        HttpAuthManager authMgr = HttpAuthManager.getInstance();
        if (!Strings.isNullOrEmpty(sessionId)) {
            SessionValue sessionValue = authMgr.getSessionValue(sessionId);
            if (sessionValue == null) {
                return false;
            }
            if (Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(sessionValue.currentUser,
                                                                      PrivPredicate.of(PrivBitSet.of(PaloPrivilege.ADMIN_PRIV,
                                                                                                     PaloPrivilege.NODE_PRIV),
                                                                                       Operator.OR))) {
                response.updateCookieAge(request, PALO_SESSION_ID, PALO_SESSION_EXPIRED_TIME);
                request.setAuthorized(true);

                ConnectContext ctx = new ConnectContext(null);
                ctx.setQualifiedUser(sessionValue.currentUser.getQualifiedUser());
                ctx.setRemoteIP(request.getHostString());
                ctx.setCurrentUserIdentity(sessionValue.currentUser);
                ctx.setCatalog(Catalog.getCurrentCatalog());
                ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
                ctx.setThreadLocalInfo();
                return true;
            }
        }
        return false;
    }

    // return true if this Action need to check password.
    // Currently, all sub actions need to check password except for MetaBaseAction.
    // if needPassword() is false, then needAdmin() should also return false
    public boolean needPassword() {
        return true;
    }

    // return true if this Action need Admin privilege.
    public boolean needAdmin() {
        return true;
    }

    protected void writeAuthResponse(BaseRequest request, BaseResponse response) {
        response.updateHeader(HttpHeaderNames.WWW_AUTHENTICATE.toString(), "Basic realm=\"\"");
        writeResponse(request, response, HttpResponseStatus.UNAUTHORIZED);
    }

    protected void writeResponse(BaseRequest request, BaseResponse response) {
        writeResponse(request, response, HttpResponseStatus.OK);
    }

    protected void addSession(BaseRequest request, BaseResponse response, SessionValue value) {
        String key = UUID.randomUUID().toString();
        DefaultCookie cookie = new DefaultCookie(PALO_SESSION_ID, key);
        cookie.setMaxAge(PALO_SESSION_EXPIRED_TIME);
        response.addCookie(cookie);
        HttpAuthManager.getInstance().addSessionValue(key, value);
    }

    protected void getPageHeader(BaseRequest request, StringBuilder sb) {
        String newPageHeaderString = PAGE_HEADER;
        newPageHeaderString = newPageHeaderString.replaceAll("<title>Apache Doris</title>",
                                                             "<title>" + Config.cluster_name + "</title>");

        sb.append(newPageHeaderString);
        sb.append(NAVIGATION_BAR_PREFIX);

        if (request.isAuthorized()) {
            sb.append("<li id=\"nav_system\"><a href=\"/system\">")
                    .append("system")
                    .append("</a></li>");
            sb.append("<li id=\"nav_backend\"><a href=\"/backend\">")
                    .append("backends")
                    .append("</a></li>");
            sb.append("<li id=\"nav_log\"><a href=\"/log\">")
                    .append("logs")
                    .append("</a></li>");
            sb.append("<li id=\"nav_query\"><a href=\"/query\">")
                    .append("queries")
                    .append("</a></li>");
            sb.append("<li id=\"nav_session\"><a href=\"/session\">")
                    .append("sessions")
                    .append("</a></li>");
            sb.append("<li id=\"nav_variable\"><a href=\"/variable\">")
                    .append("variables")
                    .append("</a></li>");
            sb.append("<li id=\"nav_ha\"><a href=\"/ha\">")
                    .append("ha")
                    .append("</a></li>");
        }
        sb.append("<li id=\"nav_help\"><a href=\"/help\">")
                .append("help")
                .append("</a></li></tr>");

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
