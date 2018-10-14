'use strict';

function DemoController($scope, $http) {

  function init() {
    $scope.samples = [
        "insert into event(id, user_id, tenant_id, app, spent, other, event_time) values(1, 1, 1, 'ps1', 3, 'Vitess1', unix_timestamp())",
        "insert into event(id, user_id, tenant_id, app, spent, other, event_time) values(2, 1, 1, 'ps1', 5, 'Vitess2', unix_timestamp()) /* different time, same month */",
        "insert into event(id, user_id, tenant_id, app, spent, other, event_time) values(3, 1, 1, 'ps1', 7, 'Vitess3', unix_timestamp()-3000000) /* different month */",
        "insert into event(id, user_id, tenant_id, app, spent, other, event_time) values(4, 1, 4, 'ps1', 11, 'Vitess4', unix_timestamp()-3000000) /* tenant_id different shard */",
        "update event set app='ps2' where id=1 /* create new entry */",
        "update event set tenant_id=4 where id=2 /* subtract from one, add to other */",
        "delete from event where id=3 /* zero-out an entry */",
        "select user_id, count(distinct app) as count from by_tenant where tenant_id = 1 and mon=201810 group by user_id order by count desc limit 10",
        "select app, count(distinct user_id) as count from by_tenant where tenant_id = 1 and mon=201810 group by app order by count desc limit 10",
        "select app, sum(spent) as spent from by_tenant where tenant_id = 1 group by app order by spent desc limit 10",
        "select avg(sum_user) from (select user_id, sum(spent) as sum_user from by_tenant where tenant_id=1 group by user_id) as su"
    ];
    $scope.submitQuery()
  }

  $scope.submitQuery = function() {
    try {
      $http({
          method: 'POST',
          url: '/cgi-bin/data2.py',
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
