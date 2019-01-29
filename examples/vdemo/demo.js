'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
        "insert into user(id, name, balance) values(1, 'sougou', 10)",
        "insert into user(id, name, balance) values(6, 'demmer', 20)",
        "insert into merchant(name, category) values('monoprice', 'electronics')",
        "insert into merchant(name, category) values('newegg', 'electronics')",
        "insert into product(id, description) values(1, 'keyboard')",
        "insert into product(id, description) values(2, 'monitor')",
        "insert into uorder(id, uid, mname, pid, price) values(1, 1, 'monoprice', 1, 10)",
        "insert into uorder(id, uid, mname, pid, price) values(2, 1, 'newegg', 2, 15)",
        "insert into uorder(id, uid, mname, pid, price) values(3, 6, 'monoprice', 2, 20)",
        "select name, description from user u join uorder o on u.id = o.uid join product p on o.pid = p.id",
        "insert into product(id, description) values(3, 'mouse')",
        "select name, description from user u join uorder o on u.id = o.uid join uproduct p on o.pid = p.id",
        "select name, category, o.id from merchant m join uorder o on m.name = o.mname",
        "select name, category, o.id from merchant m join morder o on m.name = o.mname",
        "update uorder set mname='newegg' where id=1",
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
