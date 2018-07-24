'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
        "insert into user(id, name, balance) values(1, 'sougou', 10)",
        "insert into user(id, name, balance) values(6, 'demmer', 20)",
        "insert into merchant(id, name, category) values(1, 'frys', 'electronics')",
        "insert into merchant(id, name, category) values(6, 'best buy', 'electronics')",
        "insert into product(id, description) values(1, 'keyboard')",
        "insert into product(id, description) values(2, 'monitor')",
        "insert into uorder(id, user_id, merchant_id, product_id, quantity) values(1, 1, 1, 1, 10)",
        "insert into uorder(id, user_id, merchant_id, product_id, quantity) values(2, 1, 6, 2, 5)",
        "insert into uorder(id, user_id, merchant_id, product_id, quantity) values(3, 6, 1, 2, 10)",
        "select name, description, quantity from user u join uorder o on u.id = o.user_id join product p on o.product_id = p.id",
        "insert into product(id, description) values(3, 'mouse')",
        "select name, description, quantity from user u join uorder o on u.id = o.user_id join uproduct p on o.product_id = p.id",
        "select * from user where name = 'sougou'",
        "select name, category, o.id, o.quantity from merchant m join uorder o on m.id = o.merchant_id",
        "select name, category, o.id, o.quantity from merchant m join morder o on m.id = o.merchant_id",
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
