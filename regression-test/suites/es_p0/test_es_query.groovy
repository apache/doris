suite("test_es_query", "p0") {

    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String es_6_port = context.config.otherConfigs.get("es_6_port")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists es6;"""
        sql """drop catalog if exists es7;"""
        sql """drop catalog if exists es8;"""
        sql """
            create catalog es6
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://127.0.0.1:$es_6_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
            """
        sql """
            create catalog es7
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://127.0.0.1:$es_7_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="true"
            );
            """
        sql """
            create catalog es8
            properties (
                "type"="es",
                "elasticsearch.hosts"="http://127.0.0.1:$es_8_port",
                "elasticsearch.nodes_discovery"="false",
                "elasticsearch.keyword_sniff"="false"
            );
            """
        sql """switch es6"""
        order_qt_sql1 """show tables"""
        order_qt_sql1 """select * from test1 where test2='text#1'"""
        sql """switch es8"""
        order_qt_sql1 """select * from test1 where test2='text'"""
    }
}