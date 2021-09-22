package org.apache.doris.flink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * @author wudi
 * @email wud3@shuhaisc.com
 * @date 2021/9/19
 * @description
 **/
public class TTest {
    public static void main(String[] args) throws JsonProcessingException {
        List list = Lists.newArrayList();
        ObjectMapper obj = new ObjectMapper();

        obj.createObjectNode().put("","");
        ObjectNode a = obj.createObjectNode().putPOJO("A", "1");

        list.add(a);
        System.out.println(obj.writeValueAsString(list));

        Map<String,Object> map  = Maps.newHashMap();
        map.put("A",2);
        JsonNode jsonNode = obj.valueToTree(map);
        System.out.println(jsonNode);
        list.add(jsonNode);
        list.add(jsonNode.toString());
        System.out.println(obj.writeValueAsString(list));

    }
}
