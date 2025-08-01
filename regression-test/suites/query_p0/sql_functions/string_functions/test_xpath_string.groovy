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

suite("test_xpath_string") {
    sql "drop table if exists xpath_string_args;"
    sql """
        create table xpath_string_args (
            k0 int,
            xml_not_null string not null,
            xml_null string null,
            xpath_not_null string not null,
            xpath_null string null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select xpath_string(xml_null, xpath_null) from xpath_string_args"
    order_qt_empty_not_nullable "select xpath_string(xml_not_null, xpath_not_null) from xpath_string_args"
    order_qt_empty_partial_nullable "select xpath_string(xml_null, xpath_not_null), xpath_string(xml_not_null, xpath_null) from xpath_string_args"

    sql "insert into xpath_string_args values (1, '<a><b>123</b></a>', null, '/a/b', null)"

    order_qt_all_null "select xpath_string(xml_null, xpath_null) from xpath_string_args"
    order_qt_all_not_null "select xpath_string(xml_not_null, xpath_not_null) from xpath_string_args"
    order_qt_partial_nullable "select xpath_string(xml_null, xpath_not_null), xpath_string(xml_not_null, xpath_null) from xpath_string_args"
    order_qt_nullable_no_null "select xpath_string(xml_null, nullable(xpath_not_null)), xpath_string(nullable(xml_not_null), xpath_null) from xpath_string_args"

    sql "truncate table xpath_string_args"

    sql """
    insert into xpath_string_args values 
        (2, '<a>123</a>', '<a>456</a>', '/a', '/a'),
        (3, '<a><b>123</b><c>456</c></a>', null, '/a/c', '/a/b'),
        (4, '<a><b>123</b><c>456</c></a>', '<a><d>789</d></a>', '/a/d', null),
        (5, '<a><b>123</b><b>456</b></a>', '<a><b>789</b></a>', '/a/b[1]', '/a/b'),
        (6, '<a><b>123</b><b>456</b></a>', null, '/a/b[2]', '/a/b[1]'),
        (7, '<a><b attr="val">123</b></a>', '<a><b attr="other">456</b></a>', '/a/b[@attr]', '/a/b[@attr="val"]'),
        (8, '<a><!-- comment -->123</a>', '<a>456</a>', '/a', null),
        (9, '<a><![CDATA[123]]></a>', null, '/a', '/a'),
        (10, '<a>123<b>456</b>789</a>', '<a><b>test</b></a>', '/a', '/a/b'),
        (11, '<a>  123  </a>', '<a>456</a>', '/a', null),
        (12, '<book><title>Intro to Hive</title><author>John Doe</author></book>', 
            '<book><title>SQL Guide</title></book>', 
            '//title/text()', 
            '//author/text()'),
        (13, '<root><user id="1"><name>Alice</name><age>25</age></user></root>',
            '<root><user id="2"><name>Bob</name></user></root>',
            '/root/user[@id="1"]/name',
            '/root/user/age'),
        (14, '<products><item price="10.99">Book</item><item price="20.99">Pen</item></products>',
            null,
            '/products/item[@price="20.99"]',
            '/products/item[1]'),
        (15, '<data><![CDATA[<nested>value</nested>]]></data>',
            '<data><plain>text</plain></data>',
            '/data',
            '//plain/text()'),
        (16, '<menu><item>Coffee<price>3.99</price></item><item>Tea<price>2.99</price></item></menu>',
            '<menu><item><price>5.99</price></item></menu>',
            '//item[price="2.99"]',
            '/menu/item[1]/price'),
        (17, '<doc><section id="1">First</section><section id="2">Second</section></doc>',
            null,
            '/doc/section[@id="2"]',
            '/doc/section[1]'),
        (18, '<list><elem pos="1">A</elem><elem pos="2">B</elem><elem pos="3">C</elem></list>',
            '<list><elem>X</elem></list>',
            '/list/elem[@pos="2"]',
            '/list/elem[last()]'),
        (19, '<nested><a><b><c>Deep</c></b></a></nested>',
            '<nested><x><y>Shallow</y></x></nested>',
            '//c',
            '/nested/x/y'),
        (20, '<mixed>Text<b>Bold</b>Normal<i>Italic</i>End</mixed>',
            '<mixed><b>Only Bold</b></mixed>',
            '/mixed',
            '//b/text()'),
        (21, '<empty></empty>',
            '<empty/>',
            '/empty',
            '/empty/text()')
    """

    order_qt_all_null "select xpath_string(xml_null, xpath_null) from xpath_string_args"
    order_qt_all_not_null "select xpath_string(xml_not_null, xpath_not_null) from xpath_string_args"
    order_qt_partial_nullable "select xpath_string(xml_null, xpath_not_null), xpath_string(xml_not_null, xpath_null) from xpath_string_args"
    order_qt_nullable_no_null "select xpath_string(xml_null, nullable(xpath_not_null)), xpath_string(nullable(xml_not_null), xpath_null) from xpath_string_args"

    /// consts. most by BE-UT
    order_qt_const_nullable "select xpath_string(xml_null, NULL), xpath_string(NULL, xpath_null) from xpath_string_args"
    order_qt_const_not_nullable "select xpath_string(xml_not_null, '/a/b'), xpath_string('<a><b>123</b></a>', xpath_not_null) from xpath_string_args"
    order_qt_const_partial_nullable "select xpath_string(xml_null, nullable('/a/b')), xpath_string(xml_not_null, nullable(xpath_null)) from xpath_string_args"
    order_qt_const_nullable_no_null "select xpath_string(nullable(xml_not_null), nullable('/a/b')), xpath_string(nullable('<a><b>123</b></a>'), nullable(xpath_not_null)) from xpath_string_args"

    order_qt_1 "select xpath_string('', '')"
    order_qt_2 "select xpath_string(NULL, NULL)"
    order_qt_3 "select xpath_string(NULL, '/a/b')"
    order_qt_4 "select xpath_string('<a><b>123</b></a>', NULL)"
    order_qt_5 "select xpath_string('<a><b>123</b></a>', '/a/b')"
    order_qt_6 "select xpath_string('<a>123</a>', '/a')"
    order_qt_7 "select xpath_string('<a><b>123</b><c>456</c></a>', '/a/c')"
    order_qt_8 "select xpath_string('<a><b>123</b><b>456</b></a>', '/a/b[1]')"
    order_qt_9 "select xpath_string('<a><b attr=\"val\">123</b></a>', '/a/b[@attr]')"
    order_qt_10 "select xpath_string('<a><!-- comment -->123</a>', '/a')"
    order_qt_11 "select xpath_string('<a><![CDATA[123]]></a>', '/a')"
    order_qt_12 "select xpath_string('<book><title>Intro to Hive</title></book>', '//title/text()')"
    order_qt_13 "select xpath_string(nullable('<a><b>123</b></a>'), nullable('/a/b'))"
    order_qt_14 "select xpath_string('<a><b>123</b></a>', nullable('/a/b'))"
    order_qt_15 "select xpath_string(nullable('<a><b>123</b></a>'), '/a/b')"
    order_qt_16 "select xpath_string('<root><user id=\"1\"><name>Alice</name></user></root>', '/root/user[@id=\"1\"]/name')"
    order_qt_17 "select xpath_string('<products><item price=\"10.99\">Book</item></products>', '/products/item[@price=\"10.99\"]')"
    order_qt_18 "select xpath_string('<menu><item><price>3.99</price></item></menu>', '//item/price/text()')"
    order_qt_19 "select xpath_string('<data><a>1</a><a>2</a><a>3</a></data>', '/data/a[last()]')"
    order_qt_20 "select xpath_string('<nested><a><b><c>Deep</c></b></a></nested>', '//c/text()')"
    order_qt_21 "select xpath_string('<mixed>Text<b>Bold</b>Normal</mixed>', '/mixed/text()')"
    order_qt_22 "select xpath_string('<doc><item pos=\"1\">First</item></doc>', '/doc/item[@pos=\"1\"]/text()')"
    order_qt_23 "select xpath_string('<test><a>x</a><b>y</b><c>z</c></test>', '/test/*[2]')"
    order_qt_24 "select xpath_string('<data><![CDATA[<nested>value</nested>]]></data>', '/data')"
    order_qt_25 "select xpath_string('<root><elem><!-- comment -->value</elem></root>', '/root/elem')"
    order_qt_26 "select xpath_string('<doc><section><title>Test</title><para>Text</para></section></doc>', '/doc/section[title=\"Test\"]/para')"
    order_qt_27 "select xpath_string('<list><item val=\"1\"/><item val=\"2\"/></list>', '/list/item[@val=\"2\"]')"
    order_qt_28 "select xpath_string('<data><group><name>A</name><value>1</value></group></data>', '/data/group[name=\"A\"]/value')"
    order_qt_29 "select xpath_string('<root><a><b>1</b></a><a><b>2</b></a></root>', '//a[b=\"2\"]/b')"
    order_qt_30 "select xpath_string('<doc><p class=\"main\">Content</p></doc>', '//p[@class=\"main\"]/text()')"

    // string function cases for xpath_string
    order_qt_str_1 "select xpath_string('<root><name>John</name><age>30</age></root>', 'substring-before(/root/name, \"o\")')"
    order_qt_str_2 "select xpath_string('<root><name>John</name><age>30</age></root>', 'substring-after(/root/name, \"o\")')"
    order_qt_str_3 "select xpath_string('<root><name>John</name></root>', 'contains(/root/name, \"oh\")')"
    order_qt_str_4 "select xpath_string('<root><name>John</name></root>', 'starts-with(/root/name, \"Jo\")')"
    order_qt_str_5 "select xpath_string('<root><name>John</name></root>', 'string-length(/root/name)')"
    order_qt_str_6 "select xpath_string('<root><name>  John  </name></root>', 'normalize-space(/root/name)')"
    order_qt_str_7 "select xpath_string('<root><desc>hello world</desc></root>', 'translate(/root/desc, \"helowrd\", \"HELOWRD\")')"
    order_qt_str_8 "select xpath_string('<root><desc>hello world</desc></root>', 'concat(substring(/root/desc,1,5), \"_test\")')"
    order_qt_str_9 "select xpath_string('<root><desc>hello world</desc></root>', 'substring(/root/desc,7)')"
    order_qt_str_10 "select xpath_string('<root><desc>hello world</desc></root>', 'substring(/root/desc,1,5)')"

    /// error cases:
    test {
        sql """ select xpath_string('wrong xml', '//a/c') """
        exception "Function xpath_string failed to parse XML string: No document element found"
    }
}
