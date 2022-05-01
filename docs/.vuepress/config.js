/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Values would be replaced automatically during the travis' building
const BUILDING_BRANCH = process.env.BRANCH || ''
const ALGOLIA_API_KEY = process.env.ALGOLIA_API_KEY || ''
const ALGOLIA_INDEX_NAME = process.env.ALGOLIA_INDEX_NAME || ''

function convertSidebar(list, path) {
  if (list.length > 0) {
      list.forEach((element, i) => {
        if (element.children) {
            convertSidebar(element.children, path + element.directoryPath)
            delete element.directoryPath
        } else {
            list[i] = path + element
        }
      });
  }
  return list
}

function buildAlgoliaSearchConfig(lang) {
  return {
    apiKey: ALGOLIA_API_KEY,
    indexName: ALGOLIA_INDEX_NAME,
    algoliaOptions: {
      facetFilters: ['lang:' + lang, 'version:' + BUILDING_BRANCH]
    }
  }
}

module.exports = {
  base: BUILDING_BRANCH.length > 0 ? '/' + BUILDING_BRANCH + '/' : '',
  locales: {
    '/en/': {
      lang: 'en',
      title: 'Apache Doris',
      description: 'Apache Doris'
    },
    '/zh-CN/': {
      lang: 'zh-CN',
      title: 'Apache Doris',
      description: 'Apache Doris'
    }
  },
  head: [
    ['meta', { name: 'theme-color', content: '#3eaf7c' }],
    ['meta', { name: 'apple-mobile-web-app-capable', content: 'yes' }],
    ['meta', { name: 'apple-mobile-web-app-status-bar-style', content: 'black' }],
    ['meta', { name: 'msapplication-TileColor', content: '#000000' }]
  ],
  title: 'Apache Doris',
  description: 'Apache Doris',
  themeConfig: {
    title: 'Doris',
    logo: '/images/doris-logo-only.png',
    search: true,
    smoothScroll: true,
    searchMaxSuggestions: 10,
    nextLinks: true,
    prevLinks: true,
    repo: 'apache/incubator-doris',
    repoLabel: 'GitHub',
    lastUpdated: 'Last Updated',
    editLinks: true,
    docsDir: 'docs',
    docsBranch: BUILDING_BRANCH,
    searchPlaceholder: 'Search',
    locales: {
      '/en/': {
        algolia: buildAlgoliaSearchConfig('en'),
        selectText: 'Languages',
        label: 'English',
        ariaLabel: 'Languages',
        editLinkText: 'Edit this page on GitHub',
        nav: [
          {
            text: 'Download', link: '/en/downloads/downloads'
          }
        ],
        sidebar: {
          '/en/': convertSidebar(require('./sidebar/en.js'), '/en/')
        },
        article: {
          metaTime: 'Date',
          metaAuthor: 'Author',
          paginationPre: 'Prev',
          paginationNext: 'Next'
        }
      },
      '/zh-CN/': {
        algolia: buildAlgoliaSearchConfig('zh-CN'),
        selectText: 'Languages',
        label: '简体中文',
        editLinkText: '在 GitHub 上编辑此页',
        nav: [
          {
            text: '下载', link: '/zh-CN/downloads/downloads'
          }
        ],
        sidebar: {
          '/zh-CN/': convertSidebar(require('./sidebar/zh-CN.js'), '/zh-CN/')
        },
        article: {
          metaTime: '时间',
          metaAuthor: '作者',
          paginationPre: '上一页',
          paginationNext: '下一页'
        }
      }
    }
  },
  plugins: [
    'reading-progress', 'plugin-back-to-top', 'plugin-medium-zoom'
  ]
};
