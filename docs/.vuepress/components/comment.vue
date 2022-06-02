<template>
  <section class="my-comment">
    <div class="redNum">
      <span
        :id="pathName"
        class="leancloud_visitors"
        data-flag-title="Your Article Title"
      >
      </span>
    </div>
    <div id="vcomments"></div>
  </section>
</template>

<script>
export default {
  name: "comment",
  data() {
    return {
      pathName: window.location.pathname,
    };
  },
  mounted() {
    const _this = this;
    const head = document.querySelector("head");
    const scriptEl = document.createElement("script");
    scriptEl.src = "/js/Valine.js"; //路径要改成自己的
    head.appendChild(scriptEl);
    const init = () => {
      new Valine({
        el: "#vcomments",
        appId: "V5NRsgEPWgJaDFJiKFErmjyp-gzGzoHsz",
        appKey: "xHjIfk6wDxqKHXkHlyldshLL",
        avatar: "monsterid",
        visitor: true,
        placeholder: "Be honest and write seriously!",
        path: _this.pathName,
      });
    };
    scriptEl.onload = init;

    const timer2 = setInterval(() => {
      try {
        const len = document.querySelectorAll(".vimg").length;
        if (len) {
          clearInterval(timer2);
          const vsubmit = document.querySelector(".vsubmit");
          vsubmit.addEventListener("click", () => {
            setTimeout(() => {
              Array.from(document.querySelectorAll(".vimg")).forEach((item) => {
                const email = item.getAttribute("email");
                if (email.includes("@qq.com")) {
                  const qq = email.split("@")[0];
                  item.src = ` https://apis.jxcxin.cn/api/qq?qq=${qq}`;
                }
              });
            }, 300);
          });

          Array.from(document.querySelectorAll(".vimg")).forEach((item) => {
            const email = item.getAttribute("email");
            if (email.includes("@qq.com")) {
              const qq = email.split("@")[0];
              item.src = ` https://apis.jxcxin.cn/api/qq?qq=${qq}`;
            }
          });
        }
      } catch (error) {}
    }, 500);
  },
};
</script>

<style scoped>
.my-comment {
  position: relative;
  border-radius: 4px;
  box-sizing: border-box;
  padding: 20px;
  margin-top: 50px;
  box-shadow: rgba(0, 0, 0, 0.35) 0px 5px 15px;
  transition: background 0.3s ease,
    transform 0.6s cubic-bezier(0.6, 0.2, 0.1, 1) 0s,
    -webkit-transform 0.6s cubic-bezier(0.6, 0.2, 0.1, 1) 0s;
  z-index: 1;
}
.my-comment:hover {
  transform: translateY(-5px);
}
#vcomments {
  margin-top: 20px;
}
.redNum {
  text-align: right;
}

.leancloud-visitors-count {
  color: red;
}
.my-comment :deep(.vnick) {
  color: red;
}
#vcomments :deep(.vat),
#vcomments :deep(a) {
  color: red;
}
</style>
