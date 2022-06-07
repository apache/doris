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
  <div class="page-footer">
    <ul class="apache-list" v-if="asfList">
      <li class="apache-item" v-for="asfItem in asfList" :key="asfItem.link">
        <a :href="asfItem.link" target="_blank">{{ asfItem.text }}</a>
      </li>
    </ul>
    <div class="footer-copyright">
      <p class="wow fadeInUp">Copyright © 2022 The Apache Software Foundation. Licensed under the Apache License, Version 2.0. Apache <br/> Doris(Incubating), Apache Incubator, Apache, the Apache feather logo, the Apache Doris(Incubating) logo and the <br/>Apache Incubator project logo are trademarks of The Apache Software Foundation.</p>
    </div>
  </div>
</template>

<script>
import { defineComponent, computed } from "vue-demi";
import { useInstance } from "@theme/helpers/composable";

export default defineComponent({
  setup(props, ctx) {
    const instance = useInstance();
    const asfList = computed(() => {
      const { $themeLocaleConfig } = instance;
      const navs = $themeLocaleConfig.nav;
      if (!navs) return
      const asf = navs.find((nav) => nav.text === "ASF");
      return asf.items;
    });

    return { asfList };
  },
});
</script>

<style lang="stylus" scoped>
.apache-list {
    display: flex;
    justify-content: center;
    flex-wrap: wrap;
    .apache-item {
        list-style none;
        padding: 0 20px;
        a {
            font-weight: normal;
            font-size: 14px;
            color: #3eaf7c !important;
            &:hover {
                color: #3eaf7c !important;
            }
        }
    }
}
.footer-copyright {
    color: #888888;
    text-align: center;
    font-size: 14px;
    p {
        line-height: 1.6;
        margin: 0 0 10px 0;
    }
}
@media (max-width: $MQMobile) {
    .apache-list {
        justify-content: flex-start;
    }
    .footer-copyright {
        padding: 0 12px;
        text-align: center;
    }
}
</style>
