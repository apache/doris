<!-- Licensed to the Apache Software Foundation (ASF) under one
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
-->
<template>
  <Common :sidebarItems="sidebarItems" v-if="renderComponent" :showModule="recoShowModule">
    <component v-if="$frontmatter.home" :is="homeCom"/>
    <Page v-else :sidebar-items="sidebarItems"/>
    <Footer v-if="$frontmatter.home" class="footer" />
  </Common>
</template>

<script>
import { defineComponent, computed, onMounted } from 'vue-demi'
import Home from '@theme/components/Home'
import HomeBlog from '@theme/components/HomeBlog'
import Page from '@theme/components/Page'
import Footer from '@theme/components/Footer'
import Common from '@theme/components/Common'
import { resolveSidebarItems } from '@theme/helpers/utils'
import moduleTransitonMixin from '@theme/mixins/moduleTransiton'
import { useInstance } from '@theme/helpers/composable'
import axios from "axios"

function convertSidebar(list, path) {
  if (list.length > 0) {
    list.forEach((element, i) => {
      if (element.children && element.directoryPath) {
        convertSidebar(element.children, path + element.directoryPath);
        delete element.directoryPath;
      } else {
        list[i] = `${path}${element}`;
      }
    });
  }
  return list;
}


export default defineComponent({
  mixins: [moduleTransitonMixin],
  components: { HomeBlog, Home, Page, Common, Footer },
  watch: {
    $route: {
      immediate: true,
      handler () {
        setTimeout((_) => {
          document.title = "Apache Doris";
        }, 0);
      }
    },
  },
  data () {
    return {
      renderComponent: false
    }
  },
  methods: {
    forceRerender() {
      this.renderComponent = false;
      this.$nextTick(() => {
        this.renderComponent = true;
      });
    },
  },
  setup (props, ctx) {
    const instance = useInstance()
     const fetchData = async () => {
      const res = await axios.get('/versions.json').then(rsp => rsp).catch(err => {
        instance.forceRerender()
      })
      if (!res || !res.data) {
        instance.forceRerender()
        return
      }
      const locales = instance.$site.themeConfig.locales
      let sidebar = {}
      let versionKeys = []
      Object.keys(locales).forEach(k => {
        const versionItems = res.data[k.replace(/\//gi, "")] || []
        versionItems.forEach((item) => {
          const version = item.text;
          const docName = version === "master" ? 'docs' : version
          if (versionKeys.indexOf(docName) === -1) {
            versionKeys.push(docName)
          }
          const path = `${k}${docName}/`;
          const sidebarList = JSON.parse(JSON.stringify(require(`../../sidebar${k}${docName}.js`)))
          sidebar[path] = convertSidebar(
            sidebarList,
            path
          );
        });
        const localSidebar = {}
        Object.keys(locales[k].sidebar).forEach(path => {
          if (versionKeys.every(v => path.indexOf(v) === -1)) {
            localSidebar[path] = locales[k].sidebar[path]
          }
        })
        instance.$site.themeConfig.locales[k].sidebar = {...localSidebar, ...sidebar}
      })
      instance.forceRerender()
    }

    onMounted(() => {
      fetchData()
    })

    const sidebarItems = computed(() => {
      const { $page, $site, $localePath } = instance
      if ($page) {
        return resolveSidebarItems(
          $page,
          $page.regularPath,
          $site,
          $localePath
        )
      } else {
        return []
      }
    })

    const homeCom = computed(() => {
      const { type } = instance.$themeConfig || {}
      if (type) {
        return type == 'blog' ? 'HomeBlog' : type
      }
      return 'Home'
    })

    return { sidebarItems, homeCom }
  }
})
</script>

<style src="prismjs/themes/prism-tomorrow.css"></style>
<style src="../styles/theme.styl" lang="stylus"></style>
