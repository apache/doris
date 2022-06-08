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
import { filterPosts, sortPostsByStickyAndDate, sortPostsByDate } from '../helpers/postData'

export default {
  computed: {
    $recoPosts () {
      let posts = this.$site.pages

      posts = filterPosts(posts, false)
      sortPostsByStickyAndDate(posts)

      return posts
    },
    $recoPostsForTimeline () {
      let pages = this.$recoPosts
      const formatPages = {}
      const formatPagesArr = []
      pages = filterPosts(pages, true)
      this.pages = pages.length == 0 ? [] : pages
      for (let i = 0, length = pages.length; i < length; i++) {
        const page = pages[i]
        const pageDateYear = dateFormat(page.frontmatter.date, 'year')
        if (formatPages[pageDateYear]) formatPages[pageDateYear].push(page)
        else {
          formatPages[pageDateYear] = [page]
        }
      }

      for (const key in formatPages) {
        const data = formatPages[key]
        sortPostsByDate(data)
        formatPagesArr.unshift({
          year: key,
          data
        })
      }

      return formatPagesArr
    },
    $categoriesList () {
      return this.$categories.list.map(category => {
        category.pages = category.pages.filter(page => {
          return page.frontmatter.publish !== false
        })
        return category
      })
    },
    $tagesList () {
      return this.$tags.list.map(tag => {
        tag.pages = tag.pages.filter(page => {
          return page.frontmatter.publish !== false
        })
        return tag
      })
    },
    $showSubSideBar () {
      const {
        $themeConfig: { subSidebar: themeSubSidebar, sidebar: themeSidebar },
        $frontmatter: { subSidebar: pageSubSidebar, sidebar: pageSidebar }
      } = this

      const headers = this.$page.headers || []

      if ([pageSubSidebar, pageSidebar].indexOf(false) > -1) {
        return false
      } else if ([pageSubSidebar, pageSidebar].indexOf('auto') > -1 && headers.length > 0) {
        return true
      } else if ([themeSubSidebar, themeSidebar].indexOf('auto') > -1 && headers.length > 0) {
        return true
      } else {
        return false
      }
    }
  }
}

function renderTime (date) {
  var dateee = new Date(date).toJSON()
  return new Date(+new Date(dateee) + 8 * 3600 * 1000).toISOString().replace(/T/g, ' ').replace(/\.[\d]{3}Z/, '').replace(/-/g, '/')
}
function dateFormat (date, type) {
  date = renderTime(date)
  const dateObj = new Date(date)
  const year = dateObj.getFullYear()
  const mon = dateObj.getMonth() + 1
  const day = dateObj.getDate()
  if (type == 'year') return year
  else return `${mon}-${day}`
}
