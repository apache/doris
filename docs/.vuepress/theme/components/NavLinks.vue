<!-- 
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
-->

<template>
  <div>
    <ParentLayout v-if="showNavBar"></ParentLayout>
  </div>
</template>
<script>
import ParentLayout from "@parent-theme/components/NavLinks.vue";
import DropdownLink from "@theme/components/DropdownLink.vue";
import axios from "axios";

export default {
  data: () => ({
    showNavBar: false
  }),
  components: {
    ParentLayout
  },
  mounted() {
    if (this.$site.themeConfig.hasFetchedVersions) return;
    this.$site.themeConfig.hasFetchedVersions = true;
    axios
      .get("/versions.json")
      .then(res => {
        Object.keys(this.$site.themeConfig.locales).forEach(k => {
          this.$site.themeConfig.locales[k].nav = this.$site.themeConfig.locales[k].nav.concat(
            res.data[k.replace(/\//gi, "")] || []
          );
        });
        this.showNavBar = true;
      })
      .catch(err => {
        this.showNavBar = true;
        console.log(err);
      });
  }
};
</script>
