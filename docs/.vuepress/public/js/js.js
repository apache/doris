$(function(){
    var _pushState = window.history.pushState;
    window.history.pushState = function() {
        setTimeout(function(){
            
            $('#app').parents('body').removeClass('youde');
            $('.blogliebiao').parents('.no-sidebar').parents('body').addClass('youde');
            $('.blogxiangqing').parents('.no-sidebar').parents('body').addClass('youde');
            $('.my-comment').parents('body').removeClass('youde');
            $('.timeline-content').parents('body').removeClass('youde');
            $('.newsBox').parents('body').removeClass('youde');
            let dhera = $('.nav-dropdown li a');
            if($('.nav-dropdown li a').hasClass("router-link-active")){
                $(this).parents('.nav-dropdown').prev().addClass('router-link-active');
            };

            let $home = $('#app .theme-reco-content > div:nth-child(1)').is(".home-blog");
            if($home){
                $("#app").addClass('home');
            }else{
                $("#app").removeClass('home');
            }
            $('.timebox').parent().addClass('time-nomar');

            let $category = $('#app .theme-container > div > div:last-child > div:last-child > ul').is(".category-wrapper");
            if($category){
                $("#app").addClass('category');
            }else{
                $("#app").removeClass('category');
            }

            let $timeline = $('#app .theme-container > div > div:last-child > div:last-child > ul').is(".timeline-content");
            if($timeline){
                $("#app").addClass('timeline');
            }else{
                $("#app").removeClass('timeline');
            }

            let $zhcate1 = window.location.pathname;
            if($zhcate1 === '/zh-CN/categories/DorisWeekly/'){
                location.reload();
                $('#app .category-wrapper li:nth-child(1) a').attr("href",'/zh-CN/categories/DorisWeekly/');
                $('#app .category-wrapper li:nth-child(2) a').attr("href",'/zh-CN/categories/PracticalCases/');
                $('#app .category-wrapper li:nth-child(3) a').attr("href",'/zh-CN/categories/ReleaseNote/');
            }
            if($zhcate1 === '/categories/DorisWeekly/'){
                location.reload();
                $('#app .category-wrapper li:nth-child(1) a').attr("href",'/categories/DorisWeekly/');
                $('#app .category-wrapper li:nth-child(2) a').attr("href",'/categories/PracticalCases/');
                $('#app .category-wrapper li:nth-child(3) a').attr("href",'/categories/ReleaseNote/');
            }
            if($zhcate1 === '/zh-CN/categories/PracticalCases/'){
                location.reload();
                $('#app .category-wrapper li:nth-child(1) a').attr("href",'/zh-CN/categories/DorisWeekly/');
                $('#app .category-wrapper li:nth-child(2) a').attr("href",'/zh-CN/categories/PracticalCases/');
                $('#app .category-wrapper li:nth-child(3) a').attr("href",'/zh-CN/categories/ReleaseNote/');
            }
            if($zhcate1 === '/categories/PracticalCases/'){
                location.reload();
                $('#app .category-wrapper li:nth-child(1) a').attr("href",'/categories/DorisWeekly/');
                $('#app .category-wrapper li:nth-child(2) a').attr("href",'/categories/PracticalCases/');
                $('#app .category-wrapper li:nth-child(3) a').attr("href",'/categories/ReleaseNote/');
            }
            if($zhcate1 === '/zh-CN/categories/ReleaseNote/'){
                location.reload();
                $('#app .category-wrapper li:nth-child(1) a').attr("href",'/zh-CN/categories/DorisWeekly/');
                $('#app .category-wrapper li:nth-child(2) a').attr("href",'/zh-CN/categories/PracticalCases/');
                $('#app .category-wrapper li:nth-child(3) a').attr("href",'/zh-CN/categories/ReleaseNote/');
            }
            if($zhcate1 === '/categories/ReleaseNote/'){
                location.reload();
                $('#app .category-wrapper li:nth-child(1) a').attr("href",'/categories/DorisWeekly/');
                $('#app .category-wrapper li:nth-child(2) a').attr("href",'/categories/PracticalCases/');
                $('#app .category-wrapper li:nth-child(3) a').attr("href",'/categories/ReleaseNote/');
            }


        },100)
        return _pushState.apply(this, arguments);
    }

    $('body').on('click','a',function(){
        var windName = window.location.pathname;
        var time = setInterval(function () {
            var m = $(".blogliebiao .nav-dropdown a").attr("href");
            if (m) {
                console.log(m);
                $(".blogliebiao .nav-dropdown a").each(function () {
                    var l_m = $(this).attr("href")
                    if (l_m == windName) {
                            $('.blogliebiao .nav-dropdown a').parents('.dropdown-item').removeClass("active")
                            $(this).parents('.dropdown-item').addClass("active")
                    }
                });
                clearInterval(time);
            }
        }, 100);
    })

});