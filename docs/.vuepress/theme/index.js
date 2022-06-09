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
const path = require('path')

// Theme API.
module.exports = (options, ctx) => ({
  alias () {
    const { themeConfig, siteConfig } = ctx
    // resolve algolia
    const isAlgoliaSearch = (
      themeConfig.algolia ||
      Object.keys(siteConfig.locales && themeConfig.locales || {})
        .some(base => themeConfig.locales[base].algolia)
    )
    return {
      '@AlgoliaSearchBox': isAlgoliaSearch
        ? path.resolve(__dirname, 'components/AlgoliaSearchBox.vue')
        : path.resolve(__dirname, 'noopModule.js'),
      '@SearchBox': path.resolve(__dirname, 'components/SearchBox.vue')
    }
  },

  plugins: [
    '@vuepress-reco/back-to-top',
    '@vuepress-reco/loading-page',
    '@vuepress-reco/pagation',
    '@vuepress-reco/comments',
    '@vuepress/active-header-links',
    ['@vuepress/medium-zoom', {
      selector: '.theme-reco-content :not(a) > img'
    }],
    '@vuepress/plugin-nprogress',
    ['@vuepress/plugin-blog', {
      permalink: '/:regular',
      frontmatters: [
        {
          id: 'tags',
          keys: ['tags'],
          path: '/tag/',
          layout: 'Tags',
          scopeLayout: 'Tag'
        },
        {
          id: 'zhCategories',
          keys: ['zhCategories'],
          path: '/zh-CN/categories/',
          layout: 'ZhCategories',
          scopeLayout: 'ZhCategory'
        },
        {
          id: 'categories',
          keys: ['categories'],
          path: '/en/categories/',
          layout: 'Categories',
          scopeLayout: 'Category'
        },
        {
          id: 'zhtimeline',
          keys: ['timeline'],
          path: '/zh-CN/timeline/',
          layout: 'ZhTimeLines',
          scopeLayout: 'ZhTimeLine'
        },
        {
          id: 'timeline',
          keys: ['timeline'],
          path: '/timeline/',
          layout: 'TimeLines',
          scopeLayout: 'TimeLine'
        }
      ]
    }],
    'vuepress-plugin-smooth-scroll',
    ['container', {
      type: 'tip',
      before: info => `<div class="custom-block tip"><p class="title">${info}</p>`,
      after: '</div>',
      defaultTitle: ''
    }],
    ['container', {
      type: 'warning',
      before: info => `<div class="custom-block warning"><p class="title">${info}</p>`,
      after: '</div>',
      defaultTitle: ''
    }],
    ['container', {
      type: 'danger',
      before: info => `<div class="custom-block danger"><p class="title">${info}</p>`,
      after: '</div>',
      defaultTitle: ''
    }],
    ['container', {
      type: 'right',
      defaultTitle: ''
    }],
    ['container', {
      type: 'theorem',
      before: info => `<div class="custom-block theorem"><p class="title">${info}</p>`,
      after: '</div>',
      defaultTitle: ''
    }],
    ['container', {
      type: 'details',
      before: info => `<details class="custom-block details">${info ? `<summary>${info}</summary>` : ''}\n`,
      after: () => '</details>\n',
      defaultTitle: {
        '/': 'See More',
        '/zh-CN/': '更多'
      }
    }]
  ]
})
