package org.apache.doris.http.controller;


import com.google.common.base.Strings;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import org.apache.doris.qe.HelpModule;
import org.apache.doris.qe.HelpTopic;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RestController
@RequestMapping("/rest/v1")
public class HelpController {

    private static final String DIV_BACKGROUND_COLOR = "#FCFCFC";

    private String queryString = null;

    @RequestMapping(path = "/help",method = RequestMethod.GET)
    public Object helpSearch(HttpServletRequest request){
        this.queryString = request.getParameter("query");
        if (Strings.isNullOrEmpty(queryString)) {
            // ATTN: according to Mysql protocol, the default query should be "contents"
            //       when you want to get server side help.
            queryString = "contents";
        } else {
            queryString = queryString.trim();
        }
        Map<String,Object> result = new HashMap<>();
        appendHelpInfo(result);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build(result);
        return entity;
    }

    private void appendHelpInfo(Map<String,Object> result) {


        appendExactMatchTopic(result);
        appendFuzzyMatchTopic(result);
        appendCategories(result);
    }

    private void appendExactMatchTopic(Map<String,Object> result) {
        HelpModule module = HelpModule.getInstance();
        HelpTopic topic = module.getTopic(queryString);
        if (topic == null) {
            result.put("error","No Exact Matching Topic.");
        } else {
            appendOneTopicInfo(result, topic);
        }
    }

    private void appendFuzzyMatchTopic(Map<String,Object> result) {
        HelpModule module = HelpModule.getInstance();
        List<String> topics = module.listTopicByKeyword(queryString);
        if (topics.isEmpty()) {
            result.put("error","No Fuzzy Matching Topic");
        } else if (topics.size() == 1) {
            result.put("title", "Find only one topic, show you the detail info below");
            appendOneTopicInfo(result, module.getTopic(topics.get(0)));
        } else {
            result.put("size", topics.size());
            result.put("datas",topics);
        }
    }

    private void appendCategories(Map<String,Object> result) {
        HelpModule module = HelpModule.getInstance();
        List<String> categories = module.listCategoryByName(queryString);
        if (categories.isEmpty()) {
            result.put("error","No Matching Category");
        } else if (categories.size() == 1) {
            result.put("title", "Find only one category, so show you the detail info below");
            List<String> topics = module.listTopicByCategory(categories.get(0));
            if (topics.size() > 0) {
                result.put("size", topics.size());
                result.put("datas",topics);
            }

            List<String> subCategories = module.listCategoryByCategory(categories.get(0));
            if (subCategories.size() > 0) {
                result.put("size", subCategories.size());
                result.put("datas",subCategories);
            }
        } else {
            result.put("size", categories.size());
            result.put("datas",categories);
        }
    }

    // The browser will combine continuous whitespace to one, we use <pre> tag to solve this issue.
    private void appendOneTopicInfo(Map<String,Object> result, HelpTopic topic) {
        result.put("topic", escapeHtmlInPreTag(topic.getName()) );
        result.put("description",escapeHtmlInPreTag(topic.getDescription()));
        result.put("example",escapeHtmlInPreTag(topic.getExample()));
        result.put("Keyword", escapeHtmlInPreTag(topic.getKeywords().toString()));
        result.put("Url", escapeHtmlInPreTag(topic.getUrl()));
    }

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

}
