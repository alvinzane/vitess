'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
        "select name, description from user u join uorder o on u.id = o.uid join product p on o.pid = p.id",
        "insert into product(id, description) values(3, 'mouse')",
        "select name, description from user u join uorder o on u.id = o.uid join product p on o.pid = p.id",
        "select name, category, o.id from merchant m join uorder o on m.name = o.mname",
        "select name, category, o.id from merchant m join orders o on m.name = o.mname",
        "update uorder set mname='newegg' where id=1",
        "select name, description from user u join orders o on u.id = o.uid join product p on o.pid = p.id",
        "select name, description from user u join orders o on u.id = o.uid join lookup.product p on o.pid = p.id",
        "select id, description, amount from product join sales on product.id = sales.pid",
        "select description, kount, amount from product join sales on product.id = sales.pid order by amount desc limit 1",
        "insert into uorder(id, uid, mname, pid, price) values(4, 6, 'monoprice', 1, 50)",
    ];
    $scope.submitQuery()
  }

  $scope.submitQuery = function() {
    try {
      $http({
          method: 'POST',
          url: '/cgi-bin/data.py',
          data: "query=" + $scope.query,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
      }).success(function(data, status, headers, config) {
        $scope.result = angular.fromJson(data);
      });
    } catch (err) {
      $scope.result.error = err.message;
    }
  };

  $scope.setQuery = function($query) {
    $scope.query = $query;
    angular.element("#query_input").focus();
  };

  init();
}
