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
  <Common class="categories-wrapper" :sidebar="false">
    <!-- 分类集合 -->
    <ModuleTransition>
      <ul v-show="recoShowModule" class="category-wrapper">
        <li
          class="category-item"
          :class="title == item.name ? 'active' : ''"
          v-for="(item, index) in categories"
          :key="index"
        >
          <router-link :to="lang + item.path">
            <span class="category-name">{{ item.name }}</span>
          </router-link>
        </li>
      </ul>
    </ModuleTransition>

    <!-- 博客列表 -->
    <ModuleTransition delay="0.08">
      <note-abstract
        v-show="recoShowModule"
        class="list"
        :data="posts"
        @paginationChange="paginationChange"
      ></note-abstract>
    </ModuleTransition>

    <ModuleTransition>
      <div class="footer-wapper">
        <PageFooter />
      </div>
    </ModuleTransition>
  </Common>
</template>

<script>
import { defineComponent, computed, ref } from "vue-demi";
import Common from "@theme/components/Common";
import NoteAbstract from "@theme/components/NoteAbstract";
import { ModuleTransition } from "@vuepress-reco/core/lib/components";
import {
  sortPostsByStickyAndDate,
  langPrefix,
  filterCategories,
  filterPosts,
} from "@theme/helpers/postData";
import { getOneColor } from "@theme/helpers/other";
import moduleTransitonMixin from "@theme/mixins/moduleTransiton";
import { useInstance } from "@theme/helpers/composable";
import PageFooter from "@theme/components/PageFooter";

export default defineComponent({
  mixins: [moduleTransitonMixin],
  components: { Common, NoteAbstract, ModuleTransition, PageFooter },

  setup(props, ctx) {
    let cLang = "";
    const instance = useInstance();
    const categories = computed(() => {
      const filterCategorieList = filterCategories(
        instance.$categories.list,
        cLang
      );
      const nav = instance.$themeConfig.locales["/"].nav;
      const blog = nav.find((item) => item.text === "Blog");
      const list = [];
      blog.items.forEach((item) => {
        const val = filterCategorieList.find(
          (c) => item.link.indexOf(c.name) > -1
        );
        list.push(val);
      });
      return list;
    });
    const lang = computed(() => cLang);
    const posts = computed(() => {
      let posts = instance.$currentCategories.pages;
      posts = filterPosts(posts, undefined, cLang);
      sortPostsByStickyAndDate(posts);
      return posts;
    });

    const title = computed(() => {
      return instance.$currentCategories.key;
    });

    const getCurrentTag = (tag) => {
      ctx.emit("currentTag", tag);
    };

    const paginationChange = (page) => {
      setTimeout(() => {
        window.scrollTo(0, 0);
      }, 100);
    };

    const show = computed(async () => {
      return new Promise((resolve) => setTimeout(() => resolve(true), 100));
    });

    return {
      posts,
      title,
      getCurrentTag,
      paginationChange,
      getOneColor,
      categories,
      lang,
      show,
    };
  },
});
</script>

<style src="../styles/theme.styl" lang="stylus"></style>
<style src="prismjs/themes/prism-tomorrow.css"></style>
<style lang="stylus" scoped>
.categories-wrapper {
  max-width: $contentWidth;
  margin: 0 auto;
  padding: 4.6rem 2.5rem 0;

  .category-wrapper {
    list-style: none;
    padding-left: 0;

    .category-item {
      vertical-align: middle;
      margin: 4px 8px 10px;
      display: inline-block;
      cursor: pointer;
      border-radius: $borderRadius;
      font-size: 13px;
      box-shadow: var(--box-shadow);
      transition: all 0.5s;
      background-color: var(--background-color);

      &:hover, &.active {
        background: $accentColor;

        a span.category-name {
          color: #fff;

          .post-num {
            color: $accentColor;
          }
        }
      }

      a {
        display: flex;
        box-sizing: border-box;
        width: 100%;
        height: 100%;
        padding: 8px 14px;
        justify-content: space-between;
        align-items: center;
        color: #666;

        .post-num {
          margin-left: 4px;
          width: 1.2rem;
          height: 1.2rem;
          text-align: center;
          line-height: 1.2rem;
          border-radius: $borderRadius;
          font-size: 0.7rem;
          color: #fff;
        }
      }
    }
  }
}
.footer-wapper {
  margin-left 288px;
  margin-top -40px;
  margin-bottom 30px;
}

@media (max-width: $MQMobile) {
  .categories-wrapper {
    padding: 4.6rem 1rem 0;
  }

  .page-edit {
    .edit-link {
      margin-bottom: 0.5rem;
    }

    .last-updated {
      font-size: 0.8em;
      float: none;
      text-align: left;
    }
  }
}
</style>
