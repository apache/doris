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
import { compareDate } from '@theme/helpers/utils'

let languageSupport = ['zh-CN', 'en'];
let lagnuageMapping = { 'zh-CN': 'zh-CN' };

/**
 * 取得语言前缀
 * @returns 
 */
export function langPrefix() {
  let lang = location.pathname.split('/').filter(item => item.length > 0)[0];
  return languageSupport.includes(lang) === false ? '' : lang;
}

/**
 * 过滤多语言分类数据
 * @param {Object} categories 
 * @param {Object} lang
 */
export function filterCategories(categories, lang) {
  let defaultLanguage = lagnuageMapping[lang] === undefined ? 'en' : lagnuageMapping[lang];
  return categories.filter(item => {
    for (let i = 0; i < item.pages; i++) {
      if (item.pages[i].frontmatter.language !== undefined &&
        item.pages[i].frontmatter.language !== defaultLanguage) return false;
    }
    return true;
  })
}

// 过滤博客数据
export function filterPosts(posts, isTimeline, lang) {
  let defaultLanguage = lagnuageMapping[lang] === undefined ? 'en' : lagnuageMapping[lang];
  posts = posts.filter((item, index) => {
    const { title, frontmatter: { home, date, publish } } = item
    // 语言处理
    if (item.frontmatter.language !== undefined &&
      item.frontmatter.language !== defaultLanguage) return false;
    // 过滤多个分类时产生的重复数据
    if (posts.indexOf(item) !== index) {
      return false
    } else {
      const someConditions = home === true || title == undefined || publish === false
      const boo = isTimeline === true
        ? !(someConditions || date === undefined)
        : !someConditions
      return boo
    }
  })
  return posts
}

// 排序博客数据
export function sortPostsByStickyAndDate(posts) {
  posts.sort((prev, next) => {
    const prevSticky = prev.frontmatter.sticky
    const nextSticky = next.frontmatter.sticky
    if (prevSticky && nextSticky) {
      return prevSticky == nextSticky ? compareDate(prev, next) : (prevSticky - nextSticky)
    } else if (prevSticky && !nextSticky) {
      return -1
    } else if (!prevSticky && nextSticky) {
      return 1
    }
    return compareDate(prev, next)
  })
}

export function sortPostsByDate(posts) {
  posts.sort((prev, next) => {
    return compareDate(prev, next)
  })
}
