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

    /// folding
    check_fold_consistency "xpath_string('', '')"
    check_fold_consistency "xpath_string(NULL, NULL)"
    check_fold_consistency "xpath_string(NULL, '/a/b')"
    check_fold_consistency "xpath_string('<a><b>123</b></a>', NULL)"
    check_fold_consistency "xpath_string('<a><b>123</b></a>', '/a/b')"
    check_fold_consistency "xpath_string('<a>123</a>', '/a')"
    check_fold_consistency "xpath_string('<a><b>123</b><c>456</c></a>', '/a/c')"
    check_fold_consistency "xpath_string('<a><b>123</b><b>456</b></a>', '/a/b[1]')"
    check_fold_consistency "xpath_string('<a><b attr=\"val\">123</b></a>', '/a/b[@attr]')"
    check_fold_consistency "xpath_string('<a><!-- comment -->123</a>', '/a')"
    check_fold_consistency "xpath_string('<a><![CDATA[123]]></a>', '/a')"
    check_fold_consistency "xpath_string('<book><title>Intro to Hive</title></book>', '//title/text()')"
    check_fold_consistency "xpath_string(nullable('<a><b>123</b></a>'), nullable('/a/b'))"
    check_fold_consistency "xpath_string('<a><b>123</b></a>', nullable('/a/b'))"
    check_fold_consistency "xpath_string(nullable('<a><b>123</b></a>'), '/a/b')"
    check_fold_consistency "xpath_string('<root><user id=\"1\"><name>Alice</name></user></root>', '/root/user[@id=\"1\"]/name')"
    check_fold_consistency "xpath_string('<products><item price=\"10.99\">Book</item></products>', '/products/item[@price=\"10.99\"]')"
    check_fold_consistency "xpath_string('<menu><item><price>3.99</price></item></menu>', '//item/price/text()')"
    check_fold_consistency "xpath_string('<data><a>1</a><a>2</a><a>3</a></data>', '/data/a[last()]')"
    check_fold_consistency "xpath_string('<nested><a><b><c>Deep</c></b></a></nested>', '//c/text()')"
    check_fold_consistency "xpath_string('<mixed>Text<b>Bold</b>Normal</mixed>', '/mixed/text()')"
    check_fold_consistency "xpath_string('<doc><item pos=\"1\">First</item></doc>', '/doc/item[@pos=\"1\"]/text()')"
    check_fold_consistency "xpath_string('<test><a>x</a><b>y</b><c>z</c></test>', '/test/*[2]')"
    check_fold_consistency "xpath_string('<data><![CDATA[<nested>value</nested>]]></data>', '/data')"
    check_fold_consistency "xpath_string('<root><elem><!-- comment -->value</elem></root>', '/root/elem')"
    check_fold_consistency "xpath_string('<doc><section><title>Test</title><para>Text</para></section></doc>', '/doc/section[title=\"Test\"]/para')"
    check_fold_consistency "xpath_string('<list><item val=\"1\"/><item val=\"2\"/></list>', '/list/item[@val=\"2\"]')"
    check_fold_consistency "xpath_string('<data><group><name>A</name><value>1</value></group></data>', '/data/group[name=\"A\"]/value')"
    check_fold_consistency "xpath_string('<root><a><b>1</b></a><a><b>2</b></a></root>', '//a[b=\"2\"]/b')"
    check_fold_consistency "xpath_string('<doc><p class=\"main\">Content</p></doc>', '//p[@class=\"main\"]/text()')"

    /// error cases:
    test {
        sql """ select xpath_string('wrong xml', '//a/c') """
        exception "Function xpath_string failed to parse XML string: No document element found"
    }
}
