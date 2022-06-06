/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

const BUILDING_BRANCH = process.env.BRANCH || "";
const ALGOLIA_API_KEY = process.env.ALGOLIA_API_KEY || "";
const ALGOLIA_INDEX_NAME = process.env.ALGOLIA_INDEX_NAME || "";

let versions = {
  en: [
    {
      text: "master",
      link: "/en/docs/get-starting/get-starting.html",
    },
  ],
  "zh-CN": [
    {
      text: "master",
      link: "/zh-CN/docs/get-starting/get-starting.html",
    },
  ],
};

try {
  versions = require("../versions.json");
} catch (error) {
  console.log(error);
}

function convertSidebar(list, path) {
  if (list.length > 0) {
    list.forEach((element, i) => {
      if (element.children) {
        convertSidebar(element.children, path + element.directoryPath);
        delete element.directoryPath;
      } else {
        list[i] = `${path}${element}`;
      }
    });
  }
  return list;
}

function buildAlgoliaSearchConfig(lang) {
  return {
    apiKey: ALGOLIA_API_KEY,
    indexName: ALGOLIA_INDEX_NAME,
    algoliaOptions: {
      facetFilters: ["lang:" + lang, "version:" + BUILDING_BRANCH],
    },
  };
}

function buildNavVersion(lang) {
  if (!versions) return [];
  const realLang = lang.replace(/\//gi, "");
  return versions[realLang];
}

function buildSidebarVersion(lang) {
  if (!versions) return [];
  const realLang = lang.replace(/\//gi, ""); // /en/ => / => ''; /zh-CN/ => zh-CN
  const versionItems = versions[realLang];
  if (!versionItems) return []
  const sideBar = {};
  versionItems.forEach((item) => {
    const version = item.text;
    const docName = version === "master" ? 'docs' : version
    const path = `/${realLang}/${docName}/`;
    sideBar[path] = convertSidebar(
      require(`./sidebar/${realLang}/${docName}.js`),
      path
    );
  });
  return sideBar;
}

module.exports = {
  base: BUILDING_BRANCH.length > 0 ? "/" + BUILDING_BRANCH + "/" : "",
  dest: "dist", // 打包文件夹名称
  locales: {
    "/en/": {
      lang: "en", // html lang属性
      title: "Apache Doris",
      description: "Apache Doris",
    },
    "/zh-CN/": {
      lang: "zh-CN",
      title: "Apache Doris",
      description: "Apache Doris",
    },
  },
  // 头部文件设置
  head: [
    // CSS样式上传
    ["link", { rel: "icon", href: "/blog-images/logo.png" }],
    [
      "link",
      {
        rel: "stylesheet",
        href: "//at.alicdn.com/t/font_3319292_bdqvc63l075.css",
      },
    ],
    [
      "link",
      {
        rel: "stylesheet",
        href: "https://cdn.jsdelivr.net/npm/animate.css@3.1.1/animate.min.css",
      },
    ],
    // meta 描述
    [
      "meta",
      {
        name: "viewport",
        content: "width=device-width,initial-scale=1,user-scalable=no",
      },
    ],
    // js 上传
    [
      "script",
      {
        type: "text/javascript",
        src: "https://cdn.jsdelivr.net/npm/jquery@2.1.4/dist/jquery.min.js",
      },
    ],
    ["script", { type: "text/javascript", src: "/js/xRoll.js" }],
    ["script", { type: "text/javascript", src: "/js/js.js" }],
  ],
  title: "Apache Doris",
  description: "Apache Doris",
  theme: "haobom",
  themeConfig: {
    mode: "light",
    modePicker: false,
    noFoundPageByTencent: false,
    locales: {
      "/en/": {
        algolia: buildAlgoliaSearchConfig("en"),
        versions: {
          text: "versions",
          icon: "doris doris-xiala",
          items: buildNavVersion("en"),
        },
        // 导航栏
        nav: [
          {
            text: "Document",
            link: "",
            name: "document",
          },
          {
            text: "Blog",
            icon: "doris doris-xiala",
            name: 'blog',
            items: [
              { text: "Doris Weekly", link: "/en/categories/DorisWeekly/" },
              { text: "Best Practice", link: "/en/categories/PracticalCases/" },
              { text: "Release Note", link: "/en/categories/ReleaseNote/" },
              {
                text: "Doris Internal",
                link: "/en/categories/DorisInternals/",
              },
            ],
          },
          {
            text: "Developer",
            link: "/en/developer/developer-guide/debug-tool.html",
          },
          {
            text: "Community",
            link: "/en/community/team.html",
          },
          {
            text: "User",
            link: "/en/userCase/user.html",
          },
          {
            text: "ASF",
            icon: "doris doris-xiala",
            items: [
              { text: "Foundation", link: "https://www.apache.org/" },
              { text: "Security", link: "https://www.apache.org/security/" },
              { text: "License", link: "https://www.apache.org/licenses/" },
              {
                text: "Events",
                link: "https://www.apache.org/events/current-event",
              },
              {
                text: "Sponsorship",
                link: "https://www.apache.org/foundation/sponsorship.html",
              },
              {
                text: "Privacy",
                link: "https://www.apache.org/foundation/policies/privacy.html",
              },
              {
                text: "Thanks",
                link: "https://www.apache.org/foundation/thanks.html",
              },
            ],
          },
          {
            text: "Downloads",
            link: "/en/downloads/downloads",
            className: "downloads",
          },
        ],

        // 指定页面侧边栏
        sidebar: {
          "/en/developer/": convertSidebar(
            require("./sidebar/en/developer.js"),
            "/en/developer/"
          ),
          "/en/community/": convertSidebar(
            require("./sidebar/en/community.js"),
            "/en/community/"
          ),
          ...buildSidebarVersion("/en/"),
        },
      },
      "/zh-CN/": {
        algolia: buildAlgoliaSearchConfig("zh-CN"),
        versions: {
          text: "versions",
          icon: "doris doris-xiala",
          items: buildNavVersion("zh-CN"),
        },
        // 导航栏
        nav: [
          {
            text: "文档",
            link: "",
            name: "document",
          },
          {
            text: "博客",
            icon: "doris doris-xiala",
            name: 'blog',
            items: [
              { text: "每周通报", link: "/zh-CN/categories/DorisWeekly/" },
              { text: "最佳实践", link: "/zh-CN/categories/PracticalCases/" },
              { text: "版本发布", link: "/zh-CN/categories/ReleaseNote/" },
              {
                text: "内核解析",
                link: "/zh-CN/categories/DorisInternals/",
              },
            ],
          },
          {
            text: "开发者",
            link: "/zh-CN/developer/developer-guide/debug-tool.html",
          },
          {
            text: "社区",
            link: "/zh-CN/community/team.html",
          },
          {
            text: "用户",
            link: "/zh-CN/userCase/user.html",
          },
          {
            text: "ASF",
            icon: "doris doris-xiala",
            items: [
              { text: "基金会", link: "https://www.apache.org/" },
              { text: "安全", link: "https://www.apache.org/security/" },
              { text: "版权", link: "https://www.apache.org/licenses/" },
              {
                text: "活动",
                link: "https://www.apache.org/events/current-event",
              },
              {
                text: "捐赠",
                link: "https://www.apache.org/foundation/sponsorship.html",
              },
              {
                text: "隐私",
                link: "https://www.apache.org/foundation/policies/privacy.html",
              },
              {
                text: "鸣谢",
                link: "https://www.apache.org/foundation/thanks.html",
              },
            ],
          },
          {
            text: "下载",
            link: "/zh-CN/downloads/downloads",
            className: "downloads",
          },
        ],

        // 指定页面侧边栏
        sidebar: {
          "/zh-CN/community/": convertSidebar(
            require("./sidebar/zh-CN/community.js"),
            "/zh-CN/community/"
          ),
          "/zh-CN/developer/": convertSidebar(
            require("./sidebar/zh-CN/developer.js"),
            "/zh-CN/developer/"
          ),
          ...buildSidebarVersion("/zh-CN/"),
        },
      },
    },

    logo: "/blog-images/logo.png", // 博客的Logo图片
    search: true, // 是否开启搜索框
    searchMaxSuggestions: 10, // 搜索的关键词深度
    // "lastUpdated": "Last Updated", // 最后更新时间 这个无所谓
    author: "", // 作者名称
    authorAvatar: "", // 作者头像
    record: "xxxx", // 这里是网站备案！
    startYear: "2022", // 网站的起始时间 效果：2022 - 2022
    subSidebar: "auto", //在所有页面中启用自动生成子侧边栏，原 sidebar 仍然兼容
    displayAllHeaders: true,
    sidebarDepth: 2,
  },
  markdown: {
    lineNumbers: true,
  },
};
