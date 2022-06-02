setTimeout(function(){
  $("#app").addClass('home');
  var wow = new WOW({
      boxClass: 'wow',
      animateClass: 'animated',
      offset: 0,
      mobile: true,
      live: true
  });
  wow.init();
},500)