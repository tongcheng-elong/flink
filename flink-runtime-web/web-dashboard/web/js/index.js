angular.module('flinkApp', ['ui.router', 'angularMoment', 'dndLists']).run(["$rootScope", function($rootScope) {
  $rootScope.sidebarVisible = false;
  return $rootScope.showSidebar = function() {
    $rootScope.sidebarVisible = !$rootScope.sidebarVisible;
    return $rootScope.sidebarClass = 'force-show';
  };
}]).value('flinkConfig', {
  jobServer: '',
  jobServer: 'http://localhost:8081/',
  "refresh-interval": 10000
}).value('watermarksConfig', {
  noWatermark: -9223372036854776000
}).run(["JobsService", "MainService", "flinkConfig", "$interval", function(JobsService, MainService, flinkConfig, $interval) {
  return MainService.loadConfig().then(function(config) {
    angular.extend(flinkConfig, config);
    JobsService.listJobs();
    return $interval(function() {
      return JobsService.listJobs();
    }, flinkConfig["refresh-interval"]);
  });
}]).config(["$uiViewScrollProvider", function($uiViewScrollProvider) {
  return $uiViewScrollProvider.useAnchorScroll();
}]).run(["$rootScope", "$state", function($rootScope, $state) {
  return $rootScope.$on('$stateChangeStart', function(event, toState, toParams, fromState) {
    if (toState.redirectTo) {
      event.preventDefault();
      return $state.go(toState.redirectTo, toParams);
    }
  });
}]).config(["$stateProvider", "$urlRouterProvider", function($stateProvider, $urlRouterProvider) {
  $stateProvider.state("overview", {
    url: "/overview",
    views: {
      main: {
        templateUrl: "partials/overview.html",
        controller: 'OverviewController'
      }
    }
  }).state("running-jobs", {
    url: "/running-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/running-jobs.html",
        controller: 'RunningJobsController'
      }
    }
  }).state("completed-jobs", {
    url: "/completed-jobs",
    views: {
      main: {
        templateUrl: "partials/jobs/completed-jobs.html",
        controller: 'CompletedJobsController'
      }
    }
  }).state("single-job", {
    url: "/jobs/{jobid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/jobs/job.html",
        controller: 'SingleJobController'
      }
    }
  }).state("single-job.plan", {
    url: "",
    redirectTo: "single-job.plan.subtasks",
    views: {
      details: {
        templateUrl: "partials/jobs/job.plan.html",
        controller: 'JobPlanController'
      }
    }
  }).state("single-job.plan.subtasks", {
    url: "",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.subtasks.html",
        controller: 'JobPlanSubtasksController'
      }
    }
  }).state("single-job.plan.metrics", {
    url: "/metrics",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.metrics.html",
        controller: 'JobPlanMetricsController'
      }
    }
  }).state("single-job.plan.watermarks", {
    url: "/watermarks",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.watermarks.html"
      }
    }
  }).state("single-job.plan.accumulators", {
    url: "/accumulators",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.accumulators.html",
        controller: 'JobPlanAccumulatorsController'
      }
    }
  }).state("single-job.plan.checkpoints", {
    url: "/checkpoints",
    redirectTo: "single-job.plan.checkpoints.overview",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.checkpoints.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.overview", {
    url: "/overview",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.overview.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.summary", {
    url: "/summary",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.summary.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.history", {
    url: "/history",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.history.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.config", {
    url: "/config",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.config.html",
        controller: 'JobPlanCheckpointsController'
      }
    }
  }).state("single-job.plan.checkpoints.details", {
    url: "/details/{checkpointId}",
    views: {
      'checkpoints-view': {
        templateUrl: "partials/jobs/job.plan.node.checkpoints.details.html",
        controller: 'JobPlanCheckpointDetailsController'
      }
    }
  }).state("single-job.plan.backpressure", {
    url: "/backpressure",
    views: {
      'node-details': {
        templateUrl: "partials/jobs/job.plan.node-list.backpressure.html",
        controller: 'JobPlanBackPressureController'
      }
    }
  }).state("single-job.timeline", {
    url: "/timeline",
    views: {
      details: {
        templateUrl: "partials/jobs/job.timeline.html"
      }
    }
  }).state("single-job.timeline.vertex", {
    url: "/{vertexId}",
    views: {
      vertex: {
        templateUrl: "partials/jobs/job.timeline.vertex.html",
        controller: 'JobTimelineVertexController'
      }
    }
  }).state("single-job.exceptions", {
    url: "/exceptions",
    views: {
      details: {
        templateUrl: "partials/jobs/job.exceptions.html",
        controller: 'JobExceptionsController'
      }
    }
  }).state("single-job.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobs/job.config.html"
      }
    }
  }).state("all-manager", {
    url: "/taskmanagers",
    views: {
      main: {
        templateUrl: "partials/taskmanager/index.html",
        controller: 'AllTaskManagersController'
      }
    }
  }).state("single-manager", {
    url: "/taskmanager/{taskmanagerid}",
    abstract: true,
    views: {
      main: {
        templateUrl: "partials/taskmanager/taskmanager.html",
        controller: 'SingleTaskManagerController'
      }
    }
  }).state("single-manager.metrics", {
    url: "/metrics",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.metrics.html"
      }
    }
  }).state("single-manager.stdout", {
    url: "/stdout",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.stdout.html",
        controller: 'SingleTaskManagerStdoutController'
      }
    }
  }).state("single-manager.log", {
    url: "/log",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.log.html",
        controller: 'SingleTaskManagerLogsController'
      }
    }
  }).state("single-manager.arthas", {
    url: "/arthas",
    views: {
      details: {
        templateUrl: "partials/taskmanager/taskmanager.arthas.html",
        controller: 'SingleTaskManagerArthasController'
      }
    }
  }).state("jobmanager", {
    url: "/jobmanager",
    views: {
      main: {
        templateUrl: "partials/jobmanager/index.html"
      }
    }
  }).state("jobmanager.config", {
    url: "/config",
    views: {
      details: {
        templateUrl: "partials/jobmanager/config.html",
        controller: 'JobManagerConfigController'
      }
    }
  }).state("jobmanager.stdout", {
    url: "/stdout",
    views: {
      details: {
        templateUrl: "partials/jobmanager/stdout.html",
        controller: 'JobManagerStdoutController'
      }
    }
  }).state("jobmanager.log", {
    url: "/log",
    views: {
      details: {
        templateUrl: "partials/jobmanager/log.html",
        controller: 'JobManagerLogsController'
      }
    }
  }).state("submit", {
    url: "/submit",
    views: {
      main: {
        templateUrl: "partials/submit.html",
        controller: "JobSubmitController"
      }
    }
  });
  return $urlRouterProvider.otherwise("/overview");
}]);

angular.module('flinkApp').directive('bsLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'label label-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('bpLabel', ["JobsService", function(JobsService) {
  return {
    transclude: true,
    replace: true,
    scope: {
      getBackPressureLabelClass: "&",
      status: "@"
    },
    template: "<span title='{{status}}' ng-class='getBackPressureLabelClass()'><ng-transclude></ng-transclude></span>",
    link: function(scope, element, attrs) {
      return scope.getBackPressureLabelClass = function() {
        return 'label label-' + JobsService.translateBackPressureLabelState(attrs.status);
      };
    }
  };
}]).directive('indicatorPrimary', ["JobsService", function(JobsService) {
  return {
    replace: true,
    scope: {
      getLabelClass: "&",
      status: '@'
    },
    template: "<i title='{{status}}' ng-class='getLabelClass()' />",
    link: function(scope, element, attrs) {
      return scope.getLabelClass = function() {
        return 'fa fa-circle indicator indicator-' + JobsService.translateLabelState(attrs.status);
      };
    }
  };
}]).directive('tableProperty', function() {
  return {
    replace: true,
    scope: {
      value: '='
    },
    template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"
  };
});

angular.module('flinkApp').filter("amDurationFormatExtended", ["angularMomentConfig", function(angularMomentConfig) {
  var amDurationFormatExtendedFilter;
  amDurationFormatExtendedFilter = function(value, format, durationFormat) {
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    return moment.duration(value, format).format(durationFormat, {
      trim: false
    });
  };
  amDurationFormatExtendedFilter.$stateful = angularMomentConfig.statefulFilters;
  return amDurationFormatExtendedFilter;
}]).filter("humanizeDuration", function() {
  return function(value, short) {
    var days, hours, minutes, ms, seconds, x;
    if (typeof value === "undefined" || value === null) {
      return "";
    }
    ms = value % 1000;
    x = Math.floor(value / 1000);
    seconds = x % 60;
    x = Math.floor(x / 60);
    minutes = x % 60;
    x = Math.floor(x / 60);
    hours = x % 24;
    x = Math.floor(x / 24);
    days = x;
    if (days === 0) {
      if (hours === 0) {
        if (minutes === 0) {
          if (seconds === 0) {
            return ms + "ms";
          } else {
            return seconds + "s ";
          }
        } else {
          return minutes + "m " + seconds + "s";
        }
      } else {
        if (short) {
          return hours + "h " + minutes + "m";
        } else {
          return hours + "h " + minutes + "m " + seconds + "s";
        }
      }
    } else {
      if (short) {
        return days + "d " + hours + "h";
      } else {
        return days + "d " + hours + "h " + minutes + "m " + seconds + "s";
      }
    }
  };
}).filter("limit", function() {
  return function(text) {
    if (text.length > 73) {
      text = text.substring(0, 35) + "..." + text.substring(text.length - 35, text.length);
    }
    return text;
  };
}).filter("humanizeText", function() {
  return function(text) {
    if (text) {
      return text.replace(/&gt;/g, ">").replace(/<br\/>/g, "");
    } else {
      return '';
    }
  };
}).filter("humanizeBytes", function() {
  return function(bytes) {
    var converter, units;
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB"];
    converter = function(value, power) {
      var base;
      base = Math.pow(1024, power);
      if (value < base) {
        return (value / base).toFixed(2) + " " + units[power];
      } else if (value < base * 1000) {
        return (value / base).toPrecision(3) + " " + units[power];
      } else {
        return converter(value, power + 1);
      }
    };
    if (typeof bytes === "undefined" || bytes === null) {
      return "";
    }
    if (bytes < 1000) {
      return bytes + " B";
    } else {
      return converter(bytes, 1);
    }
  };
}).filter("toLocaleString", function() {
  return function(text) {
    return text.toLocaleString();
  };
}).filter("toUpperCase", function() {
  return function(text) {
    return text.toUpperCase();
  };
}).filter("percentage", function() {
  return function(number) {
    return (number * 100).toFixed(0) + '%';
  };
}).filter("humanizeWatermark", ["watermarksConfig", function(watermarksConfig) {
  return function(value) {
    if (isNaN(value) || value <= watermarksConfig.noWatermark) {
      return 'No Watermark';
    } else {
      return value;
    }
  };
}]).filter("increment", function() {
  return function(number) {
    return parseInt(number) + 1;
  };
}).filter("humanizeChartNumeric", [
  'humanizeBytesFilter', 'humanizeDurationFilter', function(humanizeBytesFilter, humanizeDurationFilter) {
    return function(value, metric) {
      var return_val;
      return_val = '';
      if (value !== null) {
        if (/bytes/i.test(metric.id) && /persecond/i.test(metric.id)) {
          return_val = humanizeBytesFilter(value) + ' / s';
        } else if (/bytes/i.test(metric.id)) {
          return_val = humanizeBytesFilter(value);
        } else if (/persecond/i.test(metric.id)) {
          return_val = value + ' / s';
        } else if (/time/i.test(metric.id) || /latency/i.test(metric.id)) {
          return_val = humanizeDurationFilter(value, true);
        } else {
          return_val = value;
        }
      }
      return return_val;
    };
  }
]).filter("humanizeChartNumericTitle", [
  'humanizeDurationFilter', function(humanizeDurationFilter) {
    return function(value, metric) {
      var return_val;
      return_val = '';
      if (value !== null) {
        if (/bytes/i.test(metric.id) && /persecond/i.test(metric.id)) {
          return_val = value + ' Bytes / s';
        } else if (/bytes/i.test(metric.id)) {
          return_val = value + ' Bytes';
        } else if (/persecond/i.test(metric.id)) {
          return_val = value + ' / s';
        } else if (/time/i.test(metric.id) || /latency/i.test(metric.id)) {
          return_val = humanizeDurationFilter(value, false);
        } else {
          return_val = value;
        }
      }
      return return_val;
    };
  }
]).filter("searchMetrics", function() {
  return function(availableMetrics, query) {
    var metric, queryRegex;
    queryRegex = new RegExp(query, "gi");
    return (function() {
      var i, len, results;
      results = [];
      for (i = 0, len = availableMetrics.length; i < len; i++) {
        metric = availableMetrics[i];
        if (metric.id.match(queryRegex)) {
          results.push(metric);
        }
      }
      return results;
    })();
  };
});

angular.module('flinkApp').service('MainService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "config").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('JobManagerConfigController', ["$scope", "JobManagerConfigService", function($scope, JobManagerConfigService) {
  return JobManagerConfigService.loadConfig().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['config'] = data;
  });
}]).controller('JobManagerLogsController', ["$scope", "JobManagerLogsService", function($scope, JobManagerLogsService) {
  JobManagerLogsService.loadLogs().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['log'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerLogsService.loadLogs().then(function(data) {
      return $scope.jobmanager['log'] = data;
    });
  };
}]).controller('JobManagerStdoutController', ["$scope", "JobManagerStdoutService", function($scope, JobManagerStdoutService) {
  JobManagerStdoutService.loadStdout().then(function(data) {
    if ($scope.jobmanager == null) {
      $scope.jobmanager = {};
    }
    return $scope.jobmanager['stdout'] = data;
  });
  return $scope.reloadData = function() {
    return JobManagerStdoutService.loadStdout().then(function(data) {
      return $scope.jobmanager['stdout'] = data;
    });
  };
}]);

angular.module('flinkApp').service('JobManagerConfigService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var config;
  config = {};
  this.loadConfig = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/config").success(function(data, status, headers, config) {
      config = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerLogsService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var logs;
  logs = {};
  this.loadLogs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/log").success(function(data, status, headers, config) {
      logs = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]).service('JobManagerStdoutService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var stdout;
  stdout = {};
  this.loadStdout = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobmanager/stdout").success(function(data, status, headers, config) {
      stdout = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('RunningJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('running');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('CompletedJobsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  $scope.jobObserver = function() {
    return $scope.jobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  return $scope.jobObserver();
}]).controller('SingleJobController', ["$scope", "$state", "$stateParams", "JobsService", "MetricsService", "$rootScope", "flinkConfig", "$interval", "$q", "watermarksConfig", function($scope, $state, $stateParams, JobsService, MetricsService, $rootScope, flinkConfig, $interval, $q, watermarksConfig) {
  var getWatermarks, refresher;
  $scope.jobid = $stateParams.jobid;
  $scope.job = null;
  $scope.plan = null;
  $scope.watermarks = {};
  $scope.vertices = null;
  $scope.backPressureOperatorStats = {};
  refresher = $interval(function() {
    return JobsService.loadJob($stateParams.jobid).then(function(data) {
      $scope.job = data;
      return $scope.$broadcast('reload');
    });
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    $scope.job = null;
    $scope.plan = null;
    $scope.watermarks = {};
    $scope.vertices = null;
    $scope.backPressureOperatorStats = null;
    return $interval.cancel(refresher);
  });
  $scope.cancelJob = function(cancelEvent) {
    angular.element(cancelEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Cancelling...');
    return JobsService.cancelJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
  $scope.stopJob = function(stopEvent) {
    angular.element(stopEvent.currentTarget).removeClass("btn").removeClass("btn-default").html('Stopping...');
    return JobsService.stopJob($stateParams.jobid).then(function(data) {
      return {};
    });
  };
  JobsService.loadJob($stateParams.jobid).then(function(data) {
    $scope.job = data;
    $scope.vertices = data.vertices;
    $scope.plan = data.plan;
    return MetricsService.setupMetrics($stateParams.jobid, data.vertices);
  });
  getWatermarks = function(nodes) {
    var deferred, len, requestWatermarkForNode, watermarks;
    requestWatermarkForNode = (function(_this) {
      return function(node) {
        var deferred, i, jid, metricIds;
        deferred = $q.defer();
        jid = $scope.job.jid;
        metricIds = (function() {
          var j, ref, results;
          results = [];
          for (i = j = 0, ref = node.parallelism - 1; 0 <= ref ? j <= ref : j >= ref; i = 0 <= ref ? ++j : --j) {
            results.push(i + ".currentInputWatermark");
          }
          return results;
        })();
        MetricsService.getMetrics(jid, node.id, metricIds).then(function(metrics) {
          var key, lowWatermark, minValue, ref, subtaskIndex, value, watermarks;
          minValue = 0/0;
          watermarks = {};
          ref = metrics.values;
          for (key in ref) {
            value = ref[key];
            subtaskIndex = key.replace('.currentInputWatermark', '');
            watermarks[subtaskIndex] = value;
            if (isNaN(minValue) || value < minValue) {
              minValue = value;
            }
          }
          if (!isNaN(minValue) && minValue > watermarksConfig.noWatermark) {
            lowWatermark = minValue;
          } else {
            lowWatermark = 0/0;
          }
          return deferred.resolve({
            "lowWatermark": lowWatermark,
            "watermarks": watermarks
          });
        });
        return deferred.promise;
      };
    })(this);
    deferred = $q.defer();
    watermarks = {};
    len = nodes.length;
    angular.forEach(nodes, (function(_this) {
      return function(node, index) {
        var nodeId;
        nodeId = node.id;
        return requestWatermarkForNode(node).then(function(data) {
          watermarks[nodeId] = data;
          if (index >= len - 1) {
            return deferred.resolve(watermarks);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  $scope.hasWatermark = function(nodeid) {
    return $scope.watermarks[nodeid] && !isNaN($scope.watermarks[nodeid]["lowWatermark"]);
  };
  $scope.$watch('plan', function(newPlan) {
    if (newPlan) {
      return getWatermarks(newPlan.nodes).then(function(data) {
        return $scope.watermarks = data;
      });
    }
  });
  return $scope.$on('reload', function() {
    if ($scope.plan) {
      return getWatermarks($scope.plan.nodes).then(function(data) {
        return $scope.watermarks = data;
      });
    }
  });
}]).controller('JobPlanController', ["$scope", "$state", "$stateParams", "$window", "JobsService", function($scope, $state, $stateParams, $window, JobsService) {
  $scope.nodeid = null;
  $scope.nodeUnfolded = false;
  $scope.stateList = JobsService.stateList();
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      $scope.operatorCheckpointStats = null;
      $scope.$broadcast('reload');
      return $scope.$broadcast('node:change', $scope.nodeid);
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.operatorCheckpointStats = null;
    }
  };
  $scope.deactivateNode = function() {
    $scope.nodeid = null;
    $scope.nodeUnfolded = false;
    $scope.vertex = null;
    $scope.subtasks = null;
    $scope.accumulators = null;
    return $scope.operatorCheckpointStats = null;
  };
  return $scope.toggleFold = function() {
    return $scope.nodeUnfolded = !$scope.nodeUnfolded;
  };
}]).controller('JobPlanSubtasksController', ["$scope", "JobsService", function($scope, JobsService) {
  var getSubtasks;
  $scope.aggregate = false;
  getSubtasks = function() {
    if ($scope.aggregate) {
      return JobsService.getTaskManagers($scope.nodeid).then(function(data) {
        return $scope.taskmanagers = data;
      });
    } else {
      return JobsService.getSubtasks($scope.nodeid).then(function(data) {
        return $scope.subtasks = data;
      });
    }
  };
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.st)) {
    getSubtasks();
  }
  return $scope.$on('reload', function(event) {
    if ($scope.nodeid) {
      return getSubtasks();
    }
  });
}]).controller('JobPlanAccumulatorsController', ["$scope", "JobsService", function($scope, JobsService) {
  var getAccumulators;
  getAccumulators = function() {
    return JobsService.getAccumulators($scope.nodeid).then(function(data) {
      $scope.accumulators = data.main;
      return $scope.subtaskAccumulators = data.subtasks;
    });
  };
  if ($scope.nodeid && (!$scope.vertex || !$scope.vertex.accumulators)) {
    getAccumulators();
  }
  return $scope.$on('reload', function(event) {
    if ($scope.nodeid) {
      return getAccumulators();
    }
  });
}]).controller('JobPlanCheckpointsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  var getGeneralCheckpointStats;
  $scope.checkpointDetails = {};
  $scope.checkpointDetails.id = -1;
  JobsService.getCheckpointConfig().then(function(data) {
    return $scope.checkpointConfig = data;
  });
  getGeneralCheckpointStats = function() {
    return JobsService.getCheckpointStats().then(function(data) {
      if (data !== null) {
        return $scope.checkpointStats = data;
      }
    });
  };
  getGeneralCheckpointStats();
  return $scope.$on('reload', function(event) {
    return getGeneralCheckpointStats();
  });
}]).controller('JobPlanCheckpointDetailsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  var getCheckpointDetails, getCheckpointSubtaskDetails;
  $scope.subtaskDetails = {};
  $scope.checkpointDetails.id = $stateParams.checkpointId;
  getCheckpointDetails = function(checkpointId) {
    return JobsService.getCheckpointDetails(checkpointId).then(function(data) {
      if (data !== null) {
        return $scope.checkpoint = data;
      } else {
        return $scope.unknown_checkpoint = true;
      }
    });
  };
  getCheckpointSubtaskDetails = function(checkpointId, vertexId) {
    return JobsService.getCheckpointSubtaskDetails(checkpointId, vertexId).then(function(data) {
      if (data !== null) {
        return $scope.subtaskDetails[vertexId] = data;
      }
    });
  };
  getCheckpointDetails($stateParams.checkpointId);
  if ($scope.nodeid) {
    getCheckpointSubtaskDetails($stateParams.checkpointId, $scope.nodeid);
  }
  $scope.$on('reload', function(event) {
    getCheckpointDetails($stateParams.checkpointId);
    if ($scope.nodeid) {
      return getCheckpointSubtaskDetails($stateParams.checkpointId, $scope.nodeid);
    }
  });
  return $scope.$on('$destroy', function() {
    return $scope.checkpointDetails.id = -1;
  });
}]).controller('JobPlanBackPressureController', ["$scope", "JobsService", function($scope, JobsService) {
  var getOperatorBackPressure;
  getOperatorBackPressure = function() {
    $scope.now = Date.now();
    if ($scope.nodeid) {
      return JobsService.getOperatorBackPressure($scope.nodeid).then(function(data) {
        return $scope.backPressureOperatorStats[$scope.nodeid] = data;
      });
    }
  };
  getOperatorBackPressure();
  return $scope.$on('reload', function(event) {
    return getOperatorBackPressure();
  });
}]).controller('JobTimelineVertexController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  var getVertex;
  getVertex = function() {
    return JobsService.getVertex($stateParams.vertexId).then(function(data) {
      return $scope.vertex = data;
    });
  };
  getVertex();
  return $scope.$on('reload', function(event) {
    return getVertex();
  });
}]).controller('JobExceptionsController', ["$scope", "$state", "$stateParams", "JobsService", function($scope, $state, $stateParams, JobsService) {
  return JobsService.loadExceptions().then(function(data) {
    return $scope.exceptions = data;
  });
}]).controller('JobPropertiesController', ["$scope", "JobsService", function($scope, JobsService) {
  return $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      return JobsService.getNode(nodeid).then(function(data) {
        return $scope.node = data;
      });
    } else {
      $scope.nodeid = null;
      return $scope.node = null;
    }
  };
}]).controller('JobPlanMetricsController', ["$scope", "JobsService", "MetricsService", function($scope, JobsService, MetricsService) {
  var alphabeticalSortById, loadMetrics;
  $scope.dragging = false;
  $scope.window = MetricsService.getWindow();
  $scope.availableMetrics = null;
  $scope.$on('$destroy', function() {
    return MetricsService.unRegisterObserver();
  });
  loadMetrics = function() {
    JobsService.getVertex($scope.nodeid).then(function(data) {
      return $scope.vertex = data;
    });
    return MetricsService.getAvailableMetrics($scope.jobid, $scope.nodeid).then(function(data) {
      $scope.availableMetrics = data.sort(alphabeticalSortById);
      $scope.metrics = MetricsService.getMetricsSetup($scope.jobid, $scope.nodeid).names;
      return MetricsService.registerObserver($scope.jobid, $scope.nodeid, function(data) {
        return $scope.$broadcast("metrics:data:update", data.timestamp, data.values);
      });
    });
  };
  alphabeticalSortById = function(a, b) {
    var A, B;
    A = a.id.toLowerCase();
    B = b.id.toLowerCase();
    if (A < B) {
      return -1;
    } else if (A > B) {
      return 1;
    } else {
      return 0;
    }
  };
  $scope.dropped = function(event, index, item, external, type) {
    MetricsService.orderMetrics($scope.jobid, $scope.nodeid, item, index);
    $scope.$broadcast("metrics:refresh", item);
    loadMetrics();
    return false;
  };
  $scope.dragStart = function() {
    return $scope.dragging = true;
  };
  $scope.dragEnd = function() {
    return $scope.dragging = false;
  };
  $scope.addMetric = function(metric) {
    MetricsService.addMetric($scope.jobid, $scope.nodeid, metric.id);
    return loadMetrics();
  };
  $scope.removeMetric = function(metric) {
    MetricsService.removeMetric($scope.jobid, $scope.nodeid, metric);
    return loadMetrics();
  };
  $scope.setMetricSize = function(metric, size) {
    MetricsService.setMetricSize($scope.jobid, $scope.nodeid, metric, size);
    return loadMetrics();
  };
  $scope.setMetricView = function(metric, view) {
    MetricsService.setMetricView($scope.jobid, $scope.nodeid, metric, view);
    return loadMetrics();
  };
  $scope.getValues = function(metric) {
    return MetricsService.getValues($scope.jobid, $scope.nodeid, metric);
  };
  $scope.$on('node:change', function(event, nodeid) {
    if (!$scope.dragging) {
      return loadMetrics();
    }
  });
  if ($scope.nodeid) {
    return loadMetrics();
  }
}]);

angular.module('flinkApp').directive('vertex', ["$state", function($state) {
  return {
    template: "<svg class='timeline secondary' width='0' height='0'></svg>",
    scope: {
      data: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data.subtasks, function(subtask, i) {
          var times;
          times = [
            {
              label: "Scheduled",
              color: "#666",
              borderColor: "#555",
              starting_time: subtask.timestamps["SCHEDULED"],
              ending_time: subtask.timestamps["DEPLOYING"],
              type: 'regular'
            }, {
              label: "Deploying",
              color: "#aaa",
              borderColor: "#555",
              starting_time: subtask.timestamps["DEPLOYING"],
              ending_time: subtask.timestamps["RUNNING"],
              type: 'regular'
            }
          ];
          if (subtask.timestamps["FINISHED"] > 0) {
            times.push({
              label: "Running",
              color: "#ddd",
              borderColor: "#555",
              starting_time: subtask.timestamps["RUNNING"],
              ending_time: subtask.timestamps["FINISHED"],
              type: 'regular'
            });
          }
          return testData.push({
            label: "(" + subtask.subtask + ") " + subtask.host,
            times: times
          });
        });
        chart = d3.timeline().stack().tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("single").labelFormat(function(label) {
          return label;
        }).margin({
          left: 100,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).relativeTime();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      analyzeTime(scope.data);
    }
  };
}]).directive('timeline', ["$state", function($state) {
  return {
    template: "<svg class='timeline' width='0' height='0'></svg>",
    scope: {
      vertices: "=",
      jobid: "="
    },
    link: function(scope, elem, attrs) {
      var analyzeTime, containerW, svgEl, translateLabel;
      svgEl = elem.children()[0];
      containerW = elem.width();
      angular.element(svgEl).attr('width', containerW);
      translateLabel = function(label) {
        return label.replace("&gt;", ">");
      };
      analyzeTime = function(data) {
        var chart, svg, testData;
        d3.select(svgEl).selectAll("*").remove();
        testData = [];
        angular.forEach(data, function(vertex) {
          if (vertex['start-time'] > -1) {
            if (vertex.type === 'scheduled') {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#cccccc",
                    borderColor: "#555555",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    type: vertex.type
                  }
                ]
              });
            } else {
              return testData.push({
                times: [
                  {
                    label: translateLabel(vertex.name),
                    color: "#d9f1f7",
                    borderColor: "#62cdea",
                    starting_time: vertex['start-time'],
                    ending_time: vertex['end-time'],
                    link: vertex.id,
                    type: vertex.type
                  }
                ]
              });
            }
          }
        });
        chart = d3.timeline().stack().click(function(d, i, datum) {
          if (d.link) {
            return $state.go("single-job.timeline.vertex", {
              jobid: scope.jobid,
              vertexId: d.link
            });
          }
        }).tickFormat({
          format: d3.time.format("%L"),
          tickSize: 1
        }).prefix("main").margin({
          left: 0,
          right: 0,
          top: 0,
          bottom: 0
        }).itemHeight(30).showBorderLine().showHourTimeline();
        return svg = d3.select(svgEl).datum(testData).call(chart);
      };
      scope.$watch(attrs.vertices, function(data) {
        if (data) {
          return analyzeTime(data);
        }
      });
    }
  };
}]).directive('split', function() {
  return {
    compile: function(tElem, tAttrs) {
      return Split(tElem.children(), {
        sizes: [50, 50],
        direction: 'vertical'
      });
    }
  };
}).directive('jobPlan', ["$timeout", function($timeout) {
  return {
    template: "<svg class='graph'><g /></svg> <svg class='tmp' width='1' height='1'><g /></svg> <div class='btn-group zoom-buttons'> <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a> <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a> </div>",
    scope: {
      plan: '=',
      watermarks: '=',
      setNode: '&'
    },
    link: function(scope, elem, attrs) {
      var containerW, createEdge, createLabelEdge, createLabelNode, createNode, d3mainSvg, d3mainSvgG, d3tmpSvg, drawGraph, extendLabelNodeForIteration, g, getNodeType, isSpecialIterationNode, jobid, lastPosition, lastZoomScale, loadJsonToDagre, mainG, mainSvgElement, mainTmpElement, mainZoom, mergeWatermarks, searchForNode, shortenString, subgraphs;
      g = null;
      mainZoom = d3.behavior.zoom();
      subgraphs = [];
      jobid = attrs.jobid;
      mainSvgElement = elem.children()[0];
      mainG = elem.children().children()[0];
      mainTmpElement = elem.children()[1];
      d3mainSvg = d3.select(mainSvgElement);
      d3mainSvgG = d3.select(mainG);
      d3tmpSvg = d3.select(mainTmpElement);
      containerW = elem.width();
      angular.element(elem.children()[0]).width(containerW);
      lastZoomScale = 0;
      lastPosition = 0;
      scope.zoomIn = function() {
        var translate, v1, v2;
        if (mainZoom.scale() < 2.99) {
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() + 0.1 / (mainZoom.scale()));
          mainZoom.scale(mainZoom.scale() + 0.1);
          mainZoom.translate([v1, v2]);
          d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
          lastZoomScale = mainZoom.scale();
          return lastPosition = mainZoom.translate();
        }
      };
      scope.zoomOut = function() {
        var translate, v1, v2;
        if (mainZoom.scale() > 0.31) {
          mainZoom.scale(mainZoom.scale() - 0.1);
          translate = mainZoom.translate();
          v1 = translate[0] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          v2 = translate[1] * (mainZoom.scale() - 0.1 / (mainZoom.scale()));
          mainZoom.translate([v1, v2]);
          d3mainSvgG.attr("transform", "translate(" + v1 + "," + v2 + ") scale(" + mainZoom.scale() + ")");
          lastZoomScale = mainZoom.scale();
          return lastPosition = mainZoom.translate();
        }
      };
      createLabelEdge = function(el) {
        var labelValue;
        labelValue = "";
        if ((el.ship_strategy != null) || (el.local_strategy != null)) {
          labelValue += "<div class='edge-label'>";
          if (el.ship_strategy != null) {
            labelValue += el.ship_strategy;
          }
          if (el.temp_mode !== undefined) {
            labelValue += " (" + el.temp_mode + ")";
          }
          if (el.local_strategy !== undefined) {
            labelValue += ",<br>" + el.local_strategy;
          }
          labelValue += "</div>";
        }
        return labelValue;
      };
      isSpecialIterationNode = function(info) {
        return info === "partialSolution" || info === "nextPartialSolution" || info === "workset" || info === "nextWorkset" || info === "solutionSet" || info === "solutionDelta";
      };
      getNodeType = function(el, info) {
        if (info === "mirror") {
          return 'node-mirror';
        } else if (isSpecialIterationNode(info)) {
          return 'node-iteration';
        } else {
          return 'node-normal';
        }
      };
      createLabelNode = function(el, info, maxW, maxH) {
        var labelValue, stepName;
        labelValue = "<div href='#/jobs/" + jobid + "/vertex/" + el.id + "' class='node-label " + getNodeType(el, info) + "'>";
        if (info === "mirror") {
          labelValue += "<h3 class='node-name'>Mirror of " + el.operator + "</h3>";
        } else {
          labelValue += "<h3 class='node-name'>" + el.operator + "</h3>";
        }
        if (el.description === "") {
          labelValue += "";
        } else {
          stepName = el.description;
          stepName = shortenString(stepName);
          labelValue += "<h4 class='step-name'>" + stepName + "</h4>";
        }
        if (el.step_function != null) {
          labelValue += extendLabelNodeForIteration(el.id, maxW, maxH);
        } else {
          if (isSpecialIterationNode(info)) {
            labelValue += "<h5>" + info + " Node</h5>";
          }
          if (el.parallelism !== "") {
            labelValue += "<h5>Parallelism: " + el.parallelism + "</h5>";
          }
          if (el.lowWatermark !== undefined) {
            labelValue += "<h5>Low Watermark: " + el.lowWatermark + "</h5>";
          }
          if (!(el.operator === undefined || !el.operator_strategy)) {
            labelValue += "<h5>Operation: " + shortenString(el.operator_strategy) + "</h5>";
          }
        }
        labelValue += "</div>";
        return labelValue;
      };
      extendLabelNodeForIteration = function(id, maxW, maxH) {
        var labelValue, svgID;
        svgID = "svg-" + id;
        labelValue = "<svg class='" + svgID + "' width=" + maxW + " height=" + maxH + "><g /></svg>";
        return labelValue;
      };
      shortenString = function(s) {
        var sbr;
        if (s.charAt(0) === "<") {
          s = s.replace("<", "&lt;");
          s = s.replace(">", "&gt;");
        }
        sbr = "";
        while (s.length > 30) {
          sbr = sbr + s.substring(0, 30) + "<br>";
          s = s.substring(30, s.length);
        }
        sbr = sbr + s;
        return sbr;
      };
      createNode = function(g, data, el, isParent, maxW, maxH) {
        if (isParent == null) {
          isParent = false;
        }
        if (el.id === data.partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "partialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "partialSolution")
          });
        } else if (el.id === data.next_partial_solution) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextPartialSolution", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextPartialSolution")
          });
        } else if (el.id === data.workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "workset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "workset")
          });
        } else if (el.id === data.next_workset) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "nextWorkset", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "nextWorkset")
          });
        } else if (el.id === data.solution_set) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionSet", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionSet")
          });
        } else if (el.id === data.solution_delta) {
          return g.setNode(el.id, {
            label: createLabelNode(el, "solutionDelta", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "solutionDelta")
          });
        } else {
          return g.setNode(el.id, {
            label: createLabelNode(el, "", maxW, maxH),
            labelType: 'html',
            "class": getNodeType(el, "")
          });
        }
      };
      createEdge = function(g, data, el, existingNodes, pred) {
        return g.setEdge(pred.id, el.id, {
          label: createLabelEdge(pred),
          labelType: 'html',
          arrowhead: 'normal'
        });
      };
      loadJsonToDagre = function(g, data) {
        var el, existingNodes, isParent, k, l, len, len1, maxH, maxW, pred, r, ref, sg, toIterate;
        existingNodes = [];
        if (data.nodes != null) {
          toIterate = data.nodes;
        } else {
          toIterate = data.step_function;
          isParent = true;
        }
        for (k = 0, len = toIterate.length; k < len; k++) {
          el = toIterate[k];
          maxW = 0;
          maxH = 0;
          if (el.step_function) {
            sg = new dagreD3.graphlib.Graph({
              multigraph: true,
              compound: true
            }).setGraph({
              nodesep: 20,
              edgesep: 0,
              ranksep: 20,
              rankdir: "LR",
              marginx: 10,
              marginy: 10
            });
            subgraphs[el.id] = sg;
            loadJsonToDagre(sg, el);
            r = new dagreD3.render();
            d3tmpSvg.select('g').call(r, sg);
            maxW = sg.graph().width;
            maxH = sg.graph().height;
            angular.element(mainTmpElement).empty();
          }
          createNode(g, data, el, isParent, maxW, maxH);
          existingNodes.push(el.id);
          if (el.inputs != null) {
            ref = el.inputs;
            for (l = 0, len1 = ref.length; l < len1; l++) {
              pred = ref[l];
              createEdge(g, data, el, existingNodes, pred);
            }
          }
        }
        return g;
      };
      searchForNode = function(data, nodeID) {
        var el, i, j;
        for (i in data.nodes) {
          el = data.nodes[i];
          if (el.id === nodeID) {
            return el;
          }
          if (el.step_function != null) {
            for (j in el.step_function) {
              if (el.step_function[j].id === nodeID) {
                return el.step_function[j];
              }
            }
          }
        }
      };
      mergeWatermarks = function(data, watermarks) {
        var k, len, node, ref;
        if (!_.isEmpty(watermarks)) {
          ref = data.nodes;
          for (k = 0, len = ref.length; k < len; k++) {
            node = ref[k];
            if (watermarks[node.id] && !isNaN(watermarks[node.id]["lowWatermark"])) {
              node.lowWatermark = watermarks[node.id]["lowWatermark"];
            }
          }
        }
        return data;
      };
      lastPosition = 0;
      lastZoomScale = 0;
      drawGraph = function() {
        var i, newScale, renderer, sg, xCenterOffset, yCenterOffset;
        if (scope.plan) {
          g = new dagreD3.graphlib.Graph({
            multigraph: true,
            compound: true
          }).setGraph({
            nodesep: 70,
            edgesep: 0,
            ranksep: 50,
            rankdir: "LR",
            marginx: 40,
            marginy: 40
          });
          loadJsonToDagre(g, mergeWatermarks(scope.plan, scope.watermarks));
          d3mainSvgG.selectAll("*").remove();
          d3mainSvgG.attr("transform", "scale(" + 1 + ")");
          renderer = new dagreD3.render();
          d3mainSvgG.call(renderer, g);
          for (i in subgraphs) {
            sg = subgraphs[i];
            d3mainSvg.select('svg.svg-' + i + ' g').call(renderer, sg);
          }
          newScale = 0.5;
          xCenterOffset = Math.floor((angular.element(mainSvgElement).width() - g.graph().width * newScale) / 2);
          yCenterOffset = Math.floor((angular.element(mainSvgElement).height() - g.graph().height * newScale) / 2);
          if (lastZoomScale !== 0 && lastPosition !== 0) {
            mainZoom.scale(lastZoomScale).translate(lastPosition);
            d3mainSvgG.attr("transform", "translate(" + lastPosition + ") scale(" + lastZoomScale + ")");
          } else {
            mainZoom.scale(newScale).translate([xCenterOffset, yCenterOffset]);
            d3mainSvgG.attr("transform", "translate(" + xCenterOffset + ", " + yCenterOffset + ") scale(" + mainZoom.scale() + ")");
          }
          mainZoom.on("zoom", function() {
            var ev;
            ev = d3.event;
            lastZoomScale = ev.scale;
            lastPosition = ev.translate;
            return d3mainSvgG.attr("transform", "translate(" + lastPosition + ") scale(" + lastZoomScale + ")");
          });
          mainZoom(d3mainSvg);
          return d3mainSvgG.selectAll('.node').on('click', function(d) {
            return scope.setNode({
              nodeid: d
            });
          });
        }
      };
      scope.$watch(attrs.plan, function(newPlan) {
        if (newPlan) {
          return drawGraph();
        }
      });
      scope.$watch(attrs.watermarks, function(newWatermarks) {
        if (newWatermarks && scope.plan) {
          return drawGraph();
        }
      });
    }
  };
}]);

angular.module('flinkApp').service('JobsService', ["$http", "flinkConfig", "$log", "amMoment", "$q", "$timeout", function($http, flinkConfig, $log, amMoment, $q, $timeout) {
  var currentJob, currentPlan, deferreds, jobObservers, jobs, notifyObservers;
  currentJob = null;
  currentPlan = null;
  deferreds = {};
  jobs = {
    running: [],
    finished: [],
    cancelled: [],
    failed: []
  };
  jobObservers = [];
  notifyObservers = function() {
    return angular.forEach(jobObservers, function(callback) {
      return callback();
    });
  };
  this.registerObserver = function(callback) {
    return jobObservers.push(callback);
  };
  this.unRegisterObserver = function(callback) {
    var index;
    index = jobObservers.indexOf(callback);
    return jobObservers.splice(index, 1);
  };
  this.stateList = function() {
    return ['SCHEDULED', 'DEPLOYING', 'RUNNING', 'FINISHED', 'FAILED', 'CANCELING', 'CANCELED'];
  };
  this.translateLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'finished':
        return 'success';
      case 'failed':
        return 'danger';
      case 'scheduled':
        return 'default';
      case 'deploying':
        return 'info';
      case 'running':
        return 'primary';
      case 'canceling':
        return 'warning';
      case 'pending':
        return 'info';
      case 'total':
        return 'black';
      default:
        return 'default';
    }
  };
  this.setEndTimes = function(list) {
    return angular.forEach(list, function(item, jobKey) {
      if (!(item['end-time'] > -1)) {
        return item['end-time'] = item['start-time'] + item['duration'];
      }
    });
  };
  this.processVertices = function(data) {
    angular.forEach(data.vertices, function(vertex, i) {
      return vertex.type = 'regular';
    });
    return data.vertices.unshift({
      name: 'Scheduled',
      'start-time': data.timestamps['CREATED'],
      'end-time': data.timestamps['CREATED'] + 1,
      type: 'scheduled'
    });
  };
  this.listJobs = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/overview").success((function(_this) {
      return function(data, status, headers, config) {
        jobs.finished = [];
        jobs.running = [];
        _(data.jobs).groupBy(function(x) {
          switch (x.state.toLowerCase()) {
            case 'finished':
              return 'finished';
            case 'failed':
              return 'finished';
            case 'canceled':
              return 'finished';
            default:
              return 'running';
          }
        }).forEach(function(value, key) {
          switch (key) {
            case 'finished':
              return jobs.finished = _this.setEndTimes(value);
            case 'running':
              return jobs.running = _this.setEndTimes(value);
          }
        }).value();
        deferred.resolve(jobs);
        return notifyObservers();
      };
    })(this));
    return deferred.promise;
  };
  this.getJobs = function(type) {
    return jobs[type];
  };
  this.getAllJobs = function() {
    return jobs;
  };
  this.loadJob = function(jobid) {
    currentJob = null;
    deferreds.job = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + jobid).success((function(_this) {
      return function(data, status, headers, config) {
        _this.setEndTimes(data.vertices);
        _this.processVertices(data);
        return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/config").success(function(jobConfig) {
          data = angular.extend(data, jobConfig);
          currentJob = data;
          return deferreds.job.resolve(currentJob);
        });
      };
    })(this));
    return deferreds.job.promise;
  };
  this.getNode = function(nodeid) {
    var deferred, seekNode;
    seekNode = function(nodeid, data) {
      var j, len, node, sub;
      for (j = 0, len = data.length; j < len; j++) {
        node = data[j];
        if (node.id === nodeid) {
          return node;
        }
        if (node.step_function) {
          sub = seekNode(nodeid, node.step_function);
        }
        if (sub) {
          return sub;
        }
      }
      return null;
    };
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var foundNode;
        foundNode = seekNode(nodeid, currentJob.plan.nodes);
        foundNode.vertex = _this.seekVertex(nodeid);
        return deferred.resolve(foundNode);
      };
    })(this));
    return deferred.promise;
  };
  this.seekVertex = function(nodeid) {
    var j, len, ref, vertex;
    ref = currentJob.vertices;
    for (j = 0, len = ref.length; j < len; j++) {
      vertex = ref[j];
      if (vertex.id === nodeid) {
        return vertex;
      }
    }
    return null;
  };
  this.getVertex = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        var vertex;
        vertex = _this.seekVertex(vertexid);
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasktimes").success(function(data) {
          vertex.subtasks = data.subtasks;
          return deferred.resolve(vertex);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getSubtasks = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid).success(function(data) {
          var subtasks;
          subtasks = data.subtasks;
          return deferred.resolve(subtasks);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getTaskManagers = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/taskmanagers").success(function(data) {
          var taskmanagers;
          taskmanagers = data.taskmanagers;
          return deferred.resolve(taskmanagers);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getAccumulators = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        console.log(currentJob.jid);
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/accumulators").success(function(data) {
          var accumulators;
          accumulators = data['user-accumulators'];
          return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/subtasks/accumulators").success(function(data) {
            var subtaskAccumulators;
            subtaskAccumulators = data.subtasks;
            return deferred.resolve({
              main: accumulators,
              subtasks: subtaskAccumulators
            });
          });
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointConfig = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints/config").success(function(data) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointStats = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints").success(function(data, status, headers, config) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointDetails = function(checkpointid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints/details/" + checkpointid).success(function(data) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getCheckpointSubtaskDetails = function(checkpointid, vertexid) {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/checkpoints/details/" + checkpointid + "/subtasks/" + vertexid).success(function(data) {
          if (angular.equals({}, data)) {
            return deferred.resolve(null);
          } else {
            return deferred.resolve(data);
          }
        });
      };
    })(this));
    return deferred.promise;
  };
  this.getOperatorBackPressure = function(vertexid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/vertices/" + vertexid + "/backpressure").success((function(_this) {
      return function(data) {
        return deferred.resolve(data);
      };
    })(this));
    return deferred.promise;
  };
  this.translateBackPressureLabelState = function(state) {
    switch (state.toLowerCase()) {
      case 'in-progress':
        return 'danger';
      case 'ok':
        return 'success';
      case 'low':
        return 'warning';
      case 'high':
        return 'danger';
      default:
        return 'default';
    }
  };
  this.loadExceptions = function() {
    var deferred;
    deferred = $q.defer();
    deferreds.job.promise.then((function(_this) {
      return function(data) {
        return $http.get(flinkConfig.jobServer + "jobs/" + currentJob.jid + "/exceptions").success(function(exceptions) {
          currentJob.exceptions = exceptions;
          return deferred.resolve(exceptions);
        });
      };
    })(this));
    return deferred.promise;
  };
  this.cancelJob = function(jobid) {
    return $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/yarn-cancel");
  };
  this.stopJob = function(jobid) {
    return $http.get("jobs/" + jobid + "/yarn-stop");
  };
  return this;
}]);

angular.module('flinkApp').directive('metricsGraph', function() {
  return {
    template: '<div class="panel panel-default panel-metric"> <div class="panel-heading"> <span class="metric-title">{{metric.id}}</span> <div class="buttons"> <div class="btn-group"> <button type="button" ng-class="[btnClasses, {active: metric.size != \'big\'}]" ng-click="setSize(\'small\')">Small</button> <button type="button" ng-class="[btnClasses, {active: metric.size == \'big\'}]" ng-click="setSize(\'big\')">Big</button> </div> <a title="Remove" class="btn btn-default btn-xs remove" ng-click="removeMetric()"><i class="fa fa-close" /></a> </div> </div> <div class="panel-body"> <svg ng-if="metric.view == \'chart\'"/> <div ng-if="metric.view != \'chart\'"> <div class="metric-numeric" title="{{value | humanizeChartNumericTitle:metric}}">{{value | humanizeChartNumeric:metric}}</div> </div> </div> <div class="buttons"> <div class="btn-group"> <button type="button" ng-class="[btnClasses, {active: metric.view == \'chart\'}]" ng-click="setView(\'chart\')">Chart</button> <button type="button" ng-class="[btnClasses, {active: metric.view != \'chart\'}]" ng-click="setView(\'numeric\')">Numeric</button> </div> </div>',
    replace: true,
    scope: {
      metric: "=",
      window: "=",
      removeMetric: "&",
      setMetricSize: "=",
      setMetricView: "=",
      getValues: "&"
    },
    link: function(scope, element, attrs) {
      scope.btnClasses = ['btn', 'btn-default', 'btn-xs'];
      scope.value = null;
      scope.data = [
        {
          values: scope.getValues()
        }
      ];
      scope.options = {
        x: function(d, i) {
          return d.x;
        },
        y: function(d, i) {
          return d.y;
        },
        xTickFormat: function(d) {
          return d3.time.format('%H:%M:%S')(new Date(d));
        },
        yTickFormat: function(d) {
          var absD, found, pow, step;
          found = false;
          pow = 0;
          step = 1;
          absD = Math.abs(d);
          while (!found && pow < 50) {
            if (Math.pow(10, pow) <= absD && absD < Math.pow(10, pow + step)) {
              found = true;
            } else {
              pow += step;
            }
          }
          if (found && pow > 6) {
            return (d / Math.pow(10, pow)) + "E" + pow;
          } else {
            return "" + d;
          }
        }
      };
      scope.showChart = function() {
        return d3.select(element.find("svg")[0]).datum(scope.data).transition().duration(250).call(scope.chart);
      };
      scope.chart = nv.models.lineChart().options(scope.options).showLegend(false).margin({
        top: 15,
        left: 60,
        bottom: 30,
        right: 30
      });
      scope.chart.yAxis.showMaxMin(false);
      scope.chart.tooltip.hideDelay(0);
      scope.chart.tooltip.contentGenerator(function(obj) {
        return "<p>" + (d3.time.format('%H:%M:%S')(new Date(obj.point.x))) + " | " + obj.point.y + "</p>";
      });
      nv.utils.windowResize(scope.chart.update);
      scope.setSize = function(size) {
        return scope.setMetricSize(scope.metric, size);
      };
      scope.setView = function(view) {
        scope.setMetricView(scope.metric, view);
        if (view === 'chart') {
          return scope.showChart();
        }
      };
      if (scope.metric.view === 'chart') {
        scope.showChart();
      }
      scope.$on('metrics:data:update', function(event, timestamp, data) {
        scope.value = parseFloat(data[scope.metric.id]);
        scope.data[0].values.push({
          x: timestamp,
          y: scope.value
        });
        if (scope.data[0].values.length > scope.window) {
          scope.data[0].values.shift();
        }
        if (scope.metric.view === 'chart') {
          scope.showChart();
        }
        if (scope.metric.view === 'chart') {
          scope.chart.clearHighlights();
        }
        return scope.chart.tooltip.hidden(true);
      });
      return element.find(".metric-title").qtip({
        content: {
          text: scope.metric.id
        },
        position: {
          my: 'bottom left',
          at: 'top left'
        },
        style: {
          classes: 'qtip-light qtip-timeline-bar'
        }
      });
    }
  };
});

angular.module('flinkApp').service('MetricsService', ["$http", "$q", "flinkConfig", "$interval", function($http, $q, flinkConfig, $interval) {
  this.metrics = {};
  this.values = {};
  this.watched = {};
  this.observer = {
    jobid: null,
    nodeid: null,
    callback: null
  };
  this.refresh = $interval((function(_this) {
    return function() {
      return angular.forEach(_this.metrics, function(vertices, jobid) {
        return angular.forEach(vertices, function(metrics, nodeid) {
          var names;
          names = [];
          angular.forEach(metrics, function(metric, index) {
            return names.push(metric.id);
          });
          if (names.length > 0) {
            return _this.getMetrics(jobid, nodeid, names).then(function(values) {
              if (jobid === _this.observer.jobid && nodeid === _this.observer.nodeid) {
                if (_this.observer.callback) {
                  return _this.observer.callback(values);
                }
              }
            });
          }
        });
      });
    };
  })(this), flinkConfig["refresh-interval"]);
  this.registerObserver = function(jobid, nodeid, callback) {
    this.observer.jobid = jobid;
    this.observer.nodeid = nodeid;
    return this.observer.callback = callback;
  };
  this.unRegisterObserver = function() {
    return this.observer = {
      jobid: null,
      nodeid: null,
      callback: null
    };
  };
  this.setupMetrics = function(jobid, vertices) {
    this.setupLS();
    this.watched[jobid] = [];
    return angular.forEach(vertices, (function(_this) {
      return function(v, k) {
        if (v.id) {
          return _this.watched[jobid].push(v.id);
        }
      };
    })(this));
  };
  this.getWindow = function() {
    return 100;
  };
  this.setupLS = function() {
    if (sessionStorage.flinkMetrics == null) {
      this.saveSetup();
    }
    return this.metrics = JSON.parse(sessionStorage.flinkMetrics);
  };
  this.saveSetup = function() {
    return sessionStorage.flinkMetrics = JSON.stringify(this.metrics);
  };
  this.saveValue = function(jobid, nodeid, value) {
    if (this.values[jobid] == null) {
      this.values[jobid] = {};
    }
    if (this.values[jobid][nodeid] == null) {
      this.values[jobid][nodeid] = [];
    }
    this.values[jobid][nodeid].push(value);
    if (this.values[jobid][nodeid].length > this.getWindow()) {
      return this.values[jobid][nodeid].shift();
    }
  };
  this.getValues = function(jobid, nodeid, metricid) {
    var results;
    if (this.values[jobid] == null) {
      return [];
    }
    if (this.values[jobid][nodeid] == null) {
      return [];
    }
    results = [];
    angular.forEach(this.values[jobid][nodeid], (function(_this) {
      return function(v, k) {
        if (v.values[metricid] != null) {
          return results.push({
            x: v.timestamp,
            y: v.values[metricid]
          });
        }
      };
    })(this));
    return results;
  };
  this.setupLSFor = function(jobid, nodeid) {
    if (this.metrics[jobid] == null) {
      this.metrics[jobid] = {};
    }
    if (this.metrics[jobid][nodeid] == null) {
      return this.metrics[jobid][nodeid] = [];
    }
  };
  this.addMetric = function(jobid, nodeid, metricid) {
    this.setupLSFor(jobid, nodeid);
    this.metrics[jobid][nodeid].push({
      id: metricid,
      size: 'small',
      view: 'chart'
    });
    return this.saveSetup();
  };
  this.removeMetric = (function(_this) {
    return function(jobid, nodeid, metric) {
      var i;
      if (_this.metrics[jobid][nodeid] != null) {
        i = _this.metrics[jobid][nodeid].indexOf(metric);
        if (i === -1) {
          i = _.findIndex(_this.metrics[jobid][nodeid], {
            id: metric
          });
        }
        if (i !== -1) {
          _this.metrics[jobid][nodeid].splice(i, 1);
        }
        return _this.saveSetup();
      }
    };
  })(this);
  this.setMetricSize = (function(_this) {
    return function(jobid, nodeid, metric, size) {
      var i;
      if (_this.metrics[jobid][nodeid] != null) {
        i = _this.metrics[jobid][nodeid].indexOf(metric.id);
        if (i === -1) {
          i = _.findIndex(_this.metrics[jobid][nodeid], {
            id: metric.id
          });
        }
        if (i !== -1) {
          _this.metrics[jobid][nodeid][i] = {
            id: metric.id,
            size: size,
            view: metric.view
          };
        }
        return _this.saveSetup();
      }
    };
  })(this);
  this.setMetricView = (function(_this) {
    return function(jobid, nodeid, metric, view) {
      var i;
      if (_this.metrics[jobid][nodeid] != null) {
        i = _this.metrics[jobid][nodeid].indexOf(metric.id);
        if (i === -1) {
          i = _.findIndex(_this.metrics[jobid][nodeid], {
            id: metric.id
          });
        }
        if (i !== -1) {
          _this.metrics[jobid][nodeid][i] = {
            id: metric.id,
            size: metric.size,
            view: view
          };
        }
        return _this.saveSetup();
      }
    };
  })(this);
  this.orderMetrics = function(jobid, nodeid, item, index) {
    this.setupLSFor(jobid, nodeid);
    angular.forEach(this.metrics[jobid][nodeid], (function(_this) {
      return function(v, k) {
        if (v.id === item.id) {
          _this.metrics[jobid][nodeid].splice(k, 1);
          if (k < index) {
            return index = index - 1;
          }
        }
      };
    })(this));
    this.metrics[jobid][nodeid].splice(index, 0, item);
    return this.saveSetup();
  };
  this.getMetricsSetup = (function(_this) {
    return function(jobid, nodeid) {
      return {
        names: _.map(_this.metrics[jobid][nodeid], function(value) {
          if (_.isString(value)) {
            return {
              id: value,
              size: "small",
              view: "chart"
            };
          } else {
            return value;
          }
        })
      };
    };
  })(this);
  this.getAvailableMetrics = (function(_this) {
    return function(jobid, nodeid) {
      var deferred;
      _this.setupLSFor(jobid, nodeid);
      deferred = $q.defer();
      $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/vertices/" + nodeid + "/metrics").success(function(data) {
        var results;
        results = [];
        angular.forEach(data, function(v, k) {
          var i;
          i = _this.metrics[jobid][nodeid].indexOf(v.id);
          if (i === -1) {
            i = _.findIndex(_this.metrics[jobid][nodeid], {
              id: v.id
            });
          }
          if (i === -1) {
            return results.push(v);
          }
        });
        return deferred.resolve(results);
      });
      return deferred.promise;
    };
  })(this);
  this.getAllAvailableMetrics = (function(_this) {
    return function(jobid, nodeid) {
      var deferred;
      deferred = $q.defer();
      $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/vertices/" + nodeid + "/metrics").success(function(data) {
        return deferred.resolve(data);
      });
      return deferred.promise;
    };
  })(this);
  this.getMetrics = function(jobid, nodeid, metricIds) {
    var deferred, ids;
    deferred = $q.defer();
    ids = metricIds.join(",");
    $http.get(flinkConfig.jobServer + "jobs/" + jobid + "/vertices/" + nodeid + "/metrics?get=" + ids).success((function(_this) {
      return function(data) {
        var newValue, result;
        result = {};
        angular.forEach(data, function(v, k) {
          return result[v.id] = parseInt(v.value);
        });
        newValue = {
          timestamp: Date.now(),
          values: result
        };
        _this.saveValue(jobid, nodeid, newValue);
        return deferred.resolve(newValue);
      };
    })(this));
    return deferred.promise;
  };
  this.setupLS();
  return this;
}]);

angular.module('flinkApp').controller('JobSubmitController', ["$scope", "JobSubmitService", "$interval", "flinkConfig", "$state", "$location", function($scope, JobSubmitService, $interval, flinkConfig, $state, $location) {
  var refresh;
  $scope.yarn = $location.absUrl().indexOf("/proxy/application_") !== -1;
  $scope.loadList = function() {
    return JobSubmitService.loadJarList().then(function(data) {
      $scope.address = data.address;
      if (data.error != null) {
        $scope.noaccess = data.error;
      } else if (data.errors != null) {
        $scope.noaccess = data.errors[0];
      }
      return $scope.jars = data.files;
    });
  };
  $scope.defaultState = function() {
    $scope.plan = null;
    $scope.error = null;
    return $scope.state = {
      selected: null,
      parallelism: "",
      savepointPath: "",
      allowNonRestoredState: false,
      'entry-class': "",
      'program-args': "",
      'plan-button': "Show Plan",
      'submit-button': "Submit",
      'action-time': 0
    };
  };
  $scope.defaultState();
  $scope.uploader = {};
  $scope.loadList();
  refresh = $interval(function() {
    return $scope.loadList();
  }, flinkConfig["refresh-interval"]);
  $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
  $scope.selectJar = function(id) {
    if ($scope.state.selected === id) {
      return $scope.defaultState();
    } else {
      $scope.defaultState();
      return $scope.state.selected = id;
    }
  };
  $scope.deleteJar = function(event, id) {
    if ($scope.state.selected === id) {
      $scope.defaultState();
    }
    angular.element(event.currentTarget).removeClass("fa-remove").addClass("fa-spin fa-spinner");
    return JobSubmitService.deleteJar(id).then(function(data) {
      angular.element(event.currentTarget).removeClass("fa-spin fa-spinner").addClass("fa-remove");
      if (data.error != null) {
        return alert(data.error);
      } else if (data.errors != null) {
        return alert(data.errors[0]);
      }
    });
  };
  $scope.loadEntryClass = function(name) {
    return $scope.state['entry-class'] = name;
  };
  $scope.getPlan = function() {
    var action, queryParameters;
    if ($scope.state['plan-button'] === "Show Plan") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submit";
      $scope.state['plan-button'] = "Getting Plan";
      $scope.error = null;
      $scope.plan = null;
      queryParameters = {};
      if ($scope.state['entry-class']) {
        queryParameters['entry-class'] = $scope.state['entry-class'];
      }
      if ($scope.state.parallelism) {
        queryParameters['parallelism'] = $scope.state['parallelism'];
      }
      if ($scope.state['program-args']) {
        queryParameters['program-args'] = $scope.state['program-args'];
      }
      return JobSubmitService.getPlan($scope.state.selected, queryParameters).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['plan-button'] = "Show Plan";
          if (data.error != null) {
            $scope.error = data.error;
          } else if (data.errors != null) {
            $scope.error = data.errors[0];
          }
          return $scope.plan = data.plan;
        }
      })["catch"](function(err) {
        $scope.state['plan-button'] = "Show Plan";
        return $scope.error = err;
      });
    }
  };
  $scope.runJob = function() {
    var action, queryParameters, request;
    if ($scope.state['submit-button'] === "Submit") {
      action = new Date().getTime();
      $scope.state['action-time'] = action;
      $scope.state['submit-button'] = "Submitting";
      $scope.state['plan-button'] = "Show Plan";
      $scope.error = null;
      request = {};
      queryParameters = {};
      if ($scope.state['entry-class']) {
        request['entryClass'] = $scope.state['entry-class'];
        queryParameters['entry-class'] = $scope.state['entry-class'];
      }
      if ($scope.state.parallelism) {
        request['parallelism'] = $scope.state['parallelism'];
        queryParameters['parallelism'] = $scope.state['parallelism'];
      }
      if ($scope.state['program-args']) {
        request['programArgs'] = $scope.state['program-args'];
        queryParameters['program-args'] = $scope.state['program-args'];
      }
      if ($scope.state['savepointPath']) {
        request['savepointPath'] = $scope.state['savepointPath'];
        queryParameters['savepointPath'] = $scope.state['savepointPath'];
      }
      if ($scope.state['allowNonRestoredState']) {
        request['allowNonRestoredState'] = $scope.state['allowNonRestoredState'];
        queryParameters['allowNonRestoredState'] = $scope.state['allowNonRestoredState'];
      }
      return JobSubmitService.runJob($scope.state.selected, request, queryParameters).then(function(data) {
        if (action === $scope.state['action-time']) {
          $scope.state['submit-button'] = "Submit";
          if (data.error != null) {
            $scope.error = data.error;
          } else if (data.errors != null) {
            $scope.error = data.errors[0];
          }
          if (data.jobid != null) {
            return $state.go("single-job.plan.subtasks", {
              jobid: data.jobid
            });
          }
        }
      })["catch"](function(err) {
        $scope.state['submit-button'] = "Submit";
        return $scope.error = err;
      });
    }
  };
  $scope.nodeid = null;
  $scope.changeNode = function(nodeid) {
    if (nodeid !== $scope.nodeid) {
      $scope.nodeid = nodeid;
      $scope.vertex = null;
      $scope.subtasks = null;
      $scope.accumulators = null;
      return $scope.$broadcast('reload');
    } else {
      $scope.nodeid = null;
      $scope.nodeUnfolded = false;
      $scope.vertex = null;
      $scope.subtasks = null;
      return $scope.accumulators = null;
    }
  };
  $scope.clearFiles = function() {
    return $scope.uploader = {};
  };
  $scope.uploadFiles = function(files) {
    $scope.uploader = {};
    if (files.length === 1) {
      $scope.uploader['file'] = files[0];
      return $scope.uploader['upload'] = true;
    } else {
      return $scope.uploader['error'] = "Did ya forget to select a file?";
    }
  };
  return $scope.startUpload = function() {
    var formdata, xhr;
    if ($scope.uploader['file'] != null) {
      formdata = new FormData();
      formdata.append("jarfile", $scope.uploader['file']);
      $scope.uploader['upload'] = false;
      $scope.uploader['success'] = "Initializing upload...";
      xhr = new XMLHttpRequest();
      xhr.upload.onprogress = function(event) {
        $scope.uploader['success'] = null;
        return $scope.uploader['progress'] = parseInt(100 * event.loaded / event.total);
      };
      xhr.upload.onerror = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['error'] = "An error occurred while uploading your file";
      };
      xhr.upload.onload = function(event) {
        $scope.uploader['progress'] = null;
        return $scope.uploader['success'] = "Saving...";
      };
      xhr.onreadystatechange = function() {
        var response;
        if (xhr.readyState === 4) {
          response = JSON.parse(xhr.responseText);
          if (response.error != null) {
            $scope.uploader['error'] = response.error;
            return $scope.uploader['success'] = null;
          } else if (response.errors != null) {
            $scope.uploader['error'] = response.errors[0];
            return $scope.uploader['success'] = null;
          } else {
            return $scope.uploader['success'] = "Uploaded!";
          }
        }
      };
      xhr.open("POST", flinkConfig.jobServer + "jars/upload");
      return xhr.send(formdata);
    } else {
      return console.log("Unexpected Error. This should not happen");
    }
  };
}]).filter('getJarSelectClass', function() {
  return function(selected, actual) {
    if (selected === actual) {
      return "fa-check-square";
    } else {
      return "fa-square-o";
    }
  };
});

angular.module('flinkApp').service('JobSubmitService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadJarList = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jars/").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.deleteJar = function(id) {
    var deferred;
    deferred = $q.defer();
    $http["delete"](flinkConfig.jobServer + "jars/" + encodeURIComponent(id)).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.getPlan = function(id, args) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "jars/" + encodeURIComponent(id) + "/plan", {
      params: args
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    }).error(function(err) {
      if (err.errors != null) {
        return deferred.reject(err.errors[0]);
      } else {
        return deferred.reject(err);
      }
    });
    return deferred.promise;
  };
  this.runJob = function(id, request, queryParameters) {
    var deferred;
    deferred = $q.defer();
    $http.post(flinkConfig.jobServer + "jars/" + encodeURIComponent(id) + "/run", request, {
      params: queryParameters
    }).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    }).error(function(err) {
      if (err.errors != null) {
        return deferred.reject(err.errors[0]);
      } else {
        return deferred.reject(err);
      }
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('OverviewController', ["$scope", "OverviewService", "JobsService", "$interval", "flinkConfig", function($scope, OverviewService, JobsService, $interval, flinkConfig) {
  var refresh;
  $scope.jobObserver = function() {
    $scope.runningJobs = JobsService.getJobs('running');
    return $scope.finishedJobs = JobsService.getJobs('finished');
  };
  JobsService.registerObserver($scope.jobObserver);
  $scope.$on('$destroy', function() {
    return JobsService.unRegisterObserver($scope.jobObserver);
  });
  $scope.jobObserver();
  OverviewService.loadOverview().then(function(data) {
    return $scope.overview = data;
  });
  refresh = $interval(function() {
    return OverviewService.loadOverview().then(function(data) {
      return $scope.overview = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]);

angular.module('flinkApp').service('OverviewService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  var overview;
  overview = {};
  this.loadOverview = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "overview").success(function(data, status, headers, config) {
      overview = data;
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

angular.module('flinkApp').controller('AllTaskManagersController', ["$scope", "TaskManagersService", "$interval", "flinkConfig", function($scope, TaskManagersService, $interval, flinkConfig) {
  var refresh;
  TaskManagersService.loadManagers().then(function(data) {
    return $scope.managers = data;
  });
  refresh = $interval(function() {
    return TaskManagersService.loadManagers().then(function(data) {
      return $scope.managers = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]).controller('SingleTaskManagerController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  var refresh;
  $scope.metrics = {};
  SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
    return $scope.metrics = data;
  });
  refresh = $interval(function() {
    return SingleTaskManagerService.loadMetrics($stateParams.taskmanagerid).then(function(data) {
      return $scope.metrics = data;
    });
  }, flinkConfig["refresh-interval"]);
  return $scope.$on('$destroy', function() {
    return $interval.cancel(refresh);
  });
}]).controller('SingleTaskManagerLogsController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  $scope.log = {};
  $scope.taskmanagerid = $stateParams.taskmanagerid;
  SingleTaskManagerService.loadLogs($stateParams.taskmanagerid).then(function(data) {
    return $scope.log = data;
  });
  return $scope.reloadData = function() {
    return SingleTaskManagerService.loadLogs($stateParams.taskmanagerid).then(function(data) {
      return $scope.log = data;
    });
  };
}]).controller('SingleTaskManagerStdoutController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  $scope.stdout = {};
  $scope.taskmanagerid = $stateParams.taskmanagerid;
  SingleTaskManagerService.loadStdout($stateParams.taskmanagerid).then(function(data) {
    return $scope.stdout = data;
  });
  return $scope.reloadData = function() {
    return SingleTaskManagerService.loadStdout($stateParams.taskmanagerid).then(function(data) {
      return $scope.stdout = data;
    });
  };
}]).controller('SingleTaskManagerArthasController', ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function($scope, $stateParams, SingleTaskManagerService, $interval, flinkConfig) {
  var ws, xterm;
  $scope.testaa = "hello";
  $scope.taskmanagerid = $stateParams.taskmanagerid;
  xterm = {};
  ws = {};
  $scope.init = function() {
    var path;
    path = 'ws://127.0.0.1:3658/ws';
    ws = new WebSocket(path);
    console.log(ws);
    ws.onerror = function() {
      ws.close();
      return ws = null;
    };
    return ws.onopen = function() {
      xterm = new Terminal({
        cols: 170,
        rows: 50,
        screenReaderMode: true,
        rendererType: "canvas",
        convertEol: true
      });
      ws.onmessage = function(event) {
        if (event.type === 'message') {
          return xterm.write(event.data);
        }
      };
      xterm.open(document.getElementById('terminal'));
      xterm.on('data', function(data) {
        return ws.send(JSON.stringify({
          action: 'read',
          data: data
        }));
      });
      return ws.send(JSON.stringify({
        action: 'resize',
        cols: 170,
        rows: 50
      }));
    };
  };
  return $scope.init();
}]);

angular.module('flinkApp').service('TaskManagersService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadManagers = function() {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers").success(function(data, status, headers, config) {
      return deferred.resolve(data['taskmanagers']);
    });
    return deferred.promise;
  };
  return this;
}]).service('SingleTaskManagerService', ["$http", "flinkConfig", "$q", function($http, flinkConfig, $q) {
  this.loadMetrics = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid).success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.loadLogs = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/log").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  this.loadStdout = function(taskmanagerid) {
    var deferred;
    deferred = $q.defer();
    $http.get(flinkConfig.jobServer + "taskmanagers/" + taskmanagerid + "/stdout").success(function(data, status, headers, config) {
      return deferred.resolve(data);
    });
    return deferred.promise;
  };
  return this;
}]);

//# sourceMappingURL=data:application/json;charset=utf8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbImluZGV4LmNvZmZlZSIsImluZGV4LmpzIiwiY29tbW9uL2RpcmVjdGl2ZXMuY29mZmVlIiwiY29tbW9uL2RpcmVjdGl2ZXMuanMiLCJjb21tb24vZmlsdGVycy5jb2ZmZWUiLCJjb21tb24vZmlsdGVycy5qcyIsImNvbW1vbi9zZXJ2aWNlcy5jb2ZmZWUiLCJjb21tb24vc2VydmljZXMuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvam9ibWFuYWdlci9qb2JtYW5hZ2VyLmN0cmwuanMiLCJtb2R1bGVzL2pvYm1hbmFnZXIvam9ibWFuYWdlci5zdmMuY29mZmVlIiwibW9kdWxlcy9qb2JtYW5hZ2VyL2pvYm1hbmFnZXIuc3ZjLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuY3RybC5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5jdHJsLmpzIiwibW9kdWxlcy9qb2JzL2pvYnMuZGlyLmNvZmZlZSIsIm1vZHVsZXMvam9icy9qb2JzLmRpci5qcyIsIm1vZHVsZXMvam9icy9qb2JzLnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL2pvYnMvam9icy5zdmMuanMiLCJtb2R1bGVzL2pvYnMvbWV0cmljcy5kaXIuY29mZmVlIiwibW9kdWxlcy9qb2JzL21ldHJpY3MuZGlyLmpzIiwibW9kdWxlcy9qb2JzL21ldHJpY3Muc3ZjLmNvZmZlZSIsIm1vZHVsZXMvam9icy9tZXRyaWNzLnN2Yy5qcyIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvc3VibWl0L3N1Ym1pdC5jdHJsLmpzIiwibW9kdWxlcy9zdWJtaXQvc3VibWl0LnN2Yy5jb2ZmZWUiLCJtb2R1bGVzL3N1Ym1pdC9zdWJtaXQuc3ZjLmpzIiwibW9kdWxlcy9vdmVydmlldy9vdmVydmlldy5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuY3RybC5qcyIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvb3ZlcnZpZXcvb3ZlcnZpZXcuc3ZjLmpzIiwibW9kdWxlcy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5jdHJsLmNvZmZlZSIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuY3RybC5qcyIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuc3ZjLmNvZmZlZSIsIm1vZHVsZXMvdGFza21hbmFnZXIvdGFza21hbmFnZXIuc3ZjLmpzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiJBQWtCQSxRQUFRLE9BQU8sWUFBWSxDQUFDLGFBQWEsaUJBQWlCLGFBSXpELG1CQUFJLFNBQUMsWUFBRDtFQUNILFdBQVcsaUJBQWlCO0VDckI1QixPRHNCQSxXQUFXLGNBQWMsV0FBQTtJQUN2QixXQUFXLGlCQUFpQixDQUFDLFdBQVc7SUNyQnhDLE9Ec0JBLFdBQVcsZUFBZTs7SUFJN0IsTUFBTSxlQUFlO0VBQ3BCLFdBQVc7RUFDWCxXQUFXO0VBQ1gsb0JBQW9CO0dBS3JCLE1BQU0sb0JBQW9CO0VBSXpCLGFBQWEsQ0FBQztHQUtmLCtEQUFJLFNBQUMsYUFBYSxhQUFhLGFBQWEsV0FBeEM7RUNsQ0gsT0RtQ0EsWUFBWSxhQUFhLEtBQUssU0FBQyxRQUFEO0lBQzVCLFFBQVEsT0FBTyxhQUFhO0lBRTVCLFlBQVk7SUNuQ1osT0RxQ0EsVUFBVSxXQUFBO01DcENSLE9EcUNBLFlBQVk7T0FDWixZQUFZOztJQUlqQixpQ0FBTyxTQUFDLHVCQUFEO0VDdENOLE9EdUNBLHNCQUFzQjtJQUl2Qiw2QkFBSSxTQUFDLFlBQVksUUFBYjtFQ3pDSCxPRDBDQSxXQUFXLElBQUkscUJBQXFCLFNBQUMsT0FBTyxTQUFTLFVBQVUsV0FBM0I7SUFDbEMsSUFBRyxRQUFRLFlBQVg7TUFDRSxNQUFNO01DekNOLE9EMENBLE9BQU8sR0FBRyxRQUFRLFlBQVk7OztJQUluQyxnREFBTyxTQUFDLGdCQUFnQixvQkFBakI7RUFDTixlQUFlLE1BQU0sWUFDbkI7SUFBQSxLQUFLO0lBQ0wsT0FDRTtN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QTtJQUFBLFdBQVcsR0FBRztJQUVkLE1BQU0sSUFBSSxZQUFZLFlBQVksa0JBQWtCLGdCQUFnQixXQUNuRSxRQUFRLFNBQUMsTUFBTSxRQUFRLFNBQVMsUUFBeEI7TUN4QlAsT0R5QkEsU0FBUyxRQUFROztJQ3ZCbkIsT0R5QkEsU0FBUzs7RUN2QlgsT0R5QkE7O0FDdkJGIiwiZmlsZSI6ImluZGV4LmpzIiwic291cmNlc0NvbnRlbnQiOlsiI1xuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiNcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxuI1xuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXG4jXG5cbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcsIFsndWkucm91dGVyJywgJ2FuZ3VsYXJNb21lbnQnLCAnZG5kTGlzdHMnXSlcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuICgkcm9vdFNjb3BlKSAtPlxuICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gZmFsc2VcbiAgJHJvb3RTY29wZS5zaG93U2lkZWJhciA9IC0+XG4gICAgJHJvb3RTY29wZS5zaWRlYmFyVmlzaWJsZSA9ICEkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlXG4gICAgJHJvb3RTY29wZS5zaWRlYmFyQ2xhc3MgPSAnZm9yY2Utc2hvdydcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4udmFsdWUgJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICcnXG4gIGpvYlNlcnZlcjogJ2h0dHA6Ly9sb2NhbGhvc3Q6ODA4MS8nXG4gIFwicmVmcmVzaC1pbnRlcnZhbFwiOiAxMDAwMFxufVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi52YWx1ZSAnd2F0ZXJtYXJrc0NvbmZpZycsIHtcbiAgIyBBIHZhbHVlIG9mIChKYXZhKSBMb25nLk1JTl9WQUxVRSBpbmRpY2F0ZXMgdGhhdCB0aGVyZSBpcyBubyB3YXRlcm1hcmtcbiAgIyBhdmFpbGFibGUuIFRoaXMgaXMgcGFyc2VkIGJ5IEphdmFzY3JpcHQgYXMgdGhpcyBudW1iZXIuIFdlIGhhdmUgaXQgYXNcbiAgIyBhIGNvbnN0YW50IGhlcmUgdG8gY29tcGFyZSBhdmFpbGFibGUgd2F0ZXJtYXJrcyBhZ2FpbnN0LlxuICBub1dhdGVybWFyazogLTkyMjMzNzIwMzY4NTQ3NzYwMDBcbn1cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4ucnVuIChKb2JzU2VydmljZSwgTWFpblNlcnZpY2UsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwpIC0+XG4gIE1haW5TZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChjb25maWcpIC0+XG4gICAgYW5ndWxhci5leHRlbmQgZmxpbmtDb25maWcsIGNvbmZpZ1xuXG4gICAgSm9ic1NlcnZpY2UubGlzdEpvYnMoKVxuXG4gICAgJGludGVydmFsIC0+XG4gICAgICBKb2JzU2VydmljZS5saXN0Sm9icygpXG4gICAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkdWlWaWV3U2Nyb2xsUHJvdmlkZXIpIC0+XG4gICR1aVZpZXdTY3JvbGxQcm92aWRlci51c2VBbmNob3JTY3JvbGwoKVxuXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXG5cbi5ydW4gKCRyb290U2NvcGUsICRzdGF0ZSkgLT5cbiAgJHJvb3RTY29wZS4kb24gJyRzdGF0ZUNoYW5nZVN0YXJ0JywgKGV2ZW50LCB0b1N0YXRlLCB0b1BhcmFtcywgZnJvbVN0YXRlKSAtPlxuICAgIGlmIHRvU3RhdGUucmVkaXJlY3RUb1xuICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKVxuICAgICAgJHN0YXRlLmdvIHRvU3RhdGUucmVkaXJlY3RUbywgdG9QYXJhbXNcblxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxuXG4uY29uZmlnICgkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSAtPlxuICAkc3RhdGVQcm92aWRlci5zdGF0ZSBcIm92ZXJ2aWV3XCIsXG4gICAgdXJsOiBcIi9vdmVydmlld1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ092ZXJ2aWV3Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJydW5uaW5nLWpvYnNcIixcbiAgICB1cmw6IFwiL3J1bm5pbmctam9ic1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ1J1bm5pbmdKb2JzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJjb21wbGV0ZWQtam9ic1wiLFxuICAgIHVybDogXCIvY29tcGxldGVkLWpvYnNcIlxuICAgIHZpZXdzOlxuICAgICAgbWFpbjpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2JcIixcbiAgICB1cmw6IFwiL2pvYnMve2pvYmlkfVwiXG4gICAgYWJzdHJhY3Q6IHRydWVcbiAgICB2aWV3czpcbiAgICAgIG1haW46XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlSm9iQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW5cIixcbiAgICB1cmw6IFwiXCJcbiAgICByZWRpcmVjdFRvOiBcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLnN1YnRhc2tzXCIsXG4gICAgdXJsOiBcIlwiXG4gICAgdmlld3M6XG4gICAgICAnbm9kZS1kZXRhaWxzJzpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3Quc3VidGFza3MuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5tZXRyaWNzXCIsXG4gICAgdXJsOiBcIi9tZXRyaWNzXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5tZXRyaWNzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk1ldHJpY3NDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi53YXRlcm1hcmtzXCIsXG4gICAgdXJsOiBcIi93YXRlcm1hcmtzXCJcbiAgICB2aWV3czpcbiAgICAgICdub2RlLWRldGFpbHMnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC53YXRlcm1hcmtzLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IucGxhbi5hY2N1bXVsYXRvcnNcIixcbiAgICB1cmw6IFwiL2FjY3VtdWxhdG9yc1wiXG4gICAgdmlld3M6XG4gICAgICAnbm9kZS1kZXRhaWxzJzpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QuYWNjdW11bGF0b3JzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzXCIsXG4gICAgdXJsOiBcIi9jaGVja3BvaW50c1wiXG4gICAgcmVkaXJlY3RUbzogXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMub3ZlcnZpZXdcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmNoZWNrcG9pbnRzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMub3ZlcnZpZXdcIixcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCJcbiAgICB2aWV3czpcbiAgICAgICdjaGVja3BvaW50cy12aWV3JzpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLm92ZXJ2aWV3Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuc3VtbWFyeVwiLFxuICAgIHVybDogXCIvc3VtbWFyeVwiXG4gICAgdmlld3M6XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5jaGVja3BvaW50cy5zdW1tYXJ5Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuaGlzdG9yeVwiLFxuICAgIHVybDogXCIvaGlzdG9yeVwiXG4gICAgdmlld3M6XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5jaGVja3BvaW50cy5oaXN0b3J5Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuY29uZmlnXCIsXG4gICAgdXJsOiBcIi9jb25maWdcIlxuICAgIHZpZXdzOlxuICAgICAgJ2NoZWNrcG9pbnRzLXZpZXcnOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMuY29uZmlnLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMuZGV0YWlsc1wiLFxuICAgIHVybDogXCIvZGV0YWlscy97Y2hlY2twb2ludElkfVwiXG4gICAgdmlld3M6XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5jaGVja3BvaW50cy5kZXRhaWxzLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnREZXRhaWxzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtam9iLnBsYW4uYmFja3ByZXNzdXJlXCIsXG4gICAgdXJsOiBcIi9iYWNrcHJlc3N1cmVcIlxuICAgIHZpZXdzOlxuICAgICAgJ25vZGUtZGV0YWlscyc6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmJhY2twcmVzc3VyZS5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmVcIixcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLmh0bWxcIlxuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsXG4gICAgdXJsOiBcIi97dmVydGV4SWR9XCJcbiAgICB2aWV3czpcbiAgICAgIHZlcnRleDpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUudmVydGV4Lmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iVGltZWxpbmVWZXJ0ZXhDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLFxuICAgIHVybDogXCIvZXhjZXB0aW9uc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5leGNlcHRpb25zLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLWpvYi5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5jb25maWcuaHRtbFwiXG5cbiAgLnN0YXRlIFwiYWxsLW1hbmFnZXJcIixcbiAgICB1cmw6IFwiL3Rhc2ttYW5hZ2Vyc1wiXG4gICAgdmlld3M6XG4gICAgICBtYWluOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXJcIixcbiAgICAgIHVybDogXCIvdGFza21hbmFnZXIve3Rhc2ttYW5hZ2VyaWR9XCJcbiAgICAgIGFic3RyYWN0OiB0cnVlXG4gICAgICB2aWV3czpcbiAgICAgICAgbWFpbjpcbiAgICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5odG1sXCJcbiAgICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInNpbmdsZS1tYW5hZ2VyLm1ldHJpY3NcIixcbiAgICB1cmw6IFwiL21ldHJpY3NcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIubWV0cmljcy5odG1sXCJcblxuICAuc3RhdGUgXCJzaW5nbGUtbWFuYWdlci5zdGRvdXRcIixcbiAgICB1cmw6IFwiL3N0ZG91dFwiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5zdGRvdXQuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwic2luZ2xlLW1hbmFnZXIubG9nXCIsXG4gICAgdXJsOiBcIi9sb2dcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIubG9nLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlVGFza01hbmFnZXJMb2dzQ29udHJvbGxlcidcblxuICAuc3RhdGUgXCJzaW5nbGUtbWFuYWdlci5hcnRoYXNcIixcbiAgICB1cmw6IFwiL2FydGhhc1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5hcnRoYXMuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckFydGhhc0NvbnRyb2xsZXInXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlclwiLFxuICAgICAgdXJsOiBcIi9qb2JtYW5hZ2VyXCJcbiAgICAgIHZpZXdzOlxuICAgICAgICBtYWluOlxuICAgICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvaW5kZXguaHRtbFwiXG5cbiAgLnN0YXRlIFwiam9ibWFuYWdlci5jb25maWdcIixcbiAgICB1cmw6IFwiL2NvbmZpZ1wiXG4gICAgdmlld3M6XG4gICAgICBkZXRhaWxzOlxuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2NvbmZpZy5odG1sXCJcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJDb25maWdDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcImpvYm1hbmFnZXIuc3Rkb3V0XCIsXG4gICAgdXJsOiBcIi9zdGRvdXRcIlxuICAgIHZpZXdzOlxuICAgICAgZGV0YWlsczpcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9zdGRvdXQuaHRtbFwiXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcidcblxuICAuc3RhdGUgXCJqb2JtYW5hZ2VyLmxvZ1wiLFxuICAgIHVybDogXCIvbG9nXCJcbiAgICB2aWV3czpcbiAgICAgIGRldGFpbHM6XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvbG9nLmh0bWxcIlxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJ1xuXG4gIC5zdGF0ZSBcInN1Ym1pdFwiLFxuICAgICAgdXJsOiBcIi9zdWJtaXRcIlxuICAgICAgdmlld3M6XG4gICAgICAgIG1haW46XG4gICAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvc3VibWl0Lmh0bWxcIlxuICAgICAgICAgIGNvbnRyb2xsZXI6IFwiSm9iU3VibWl0Q29udHJvbGxlclwiXG5cbiAgJHVybFJvdXRlclByb3ZpZGVyLm90aGVyd2lzZSBcIi9vdmVydmlld1wiXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnLCBbJ3VpLnJvdXRlcicsICdhbmd1bGFyTW9tZW50JywgJ2RuZExpc3RzJ10pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlKSB7XG4gICRyb290U2NvcGUuc2lkZWJhclZpc2libGUgPSBmYWxzZTtcbiAgcmV0dXJuICRyb290U2NvcGUuc2hvd1NpZGViYXIgPSBmdW5jdGlvbigpIHtcbiAgICAkcm9vdFNjb3BlLnNpZGViYXJWaXNpYmxlID0gISRyb290U2NvcGUuc2lkZWJhclZpc2libGU7XG4gICAgcmV0dXJuICRyb290U2NvcGUuc2lkZWJhckNsYXNzID0gJ2ZvcmNlLXNob3cnO1xuICB9O1xufSkudmFsdWUoJ2ZsaW5rQ29uZmlnJywge1xuICBqb2JTZXJ2ZXI6ICcnLFxuICBqb2JTZXJ2ZXI6ICdodHRwOi8vbG9jYWxob3N0OjgwODEvJyxcbiAgXCJyZWZyZXNoLWludGVydmFsXCI6IDEwMDAwXG59KS52YWx1ZSgnd2F0ZXJtYXJrc0NvbmZpZycsIHtcbiAgbm9XYXRlcm1hcms6IC05MjIzMzcyMDM2ODU0Nzc2MDAwXG59KS5ydW4oZnVuY3Rpb24oSm9ic1NlcnZpY2UsIE1haW5TZXJ2aWNlLCBmbGlua0NvbmZpZywgJGludGVydmFsKSB7XG4gIHJldHVybiBNYWluU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihjb25maWcpIHtcbiAgICBhbmd1bGFyLmV4dGVuZChmbGlua0NvbmZpZywgY29uZmlnKTtcbiAgICBKb2JzU2VydmljZS5saXN0Sm9icygpO1xuICAgIHJldHVybiAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgICByZXR1cm4gSm9ic1NlcnZpY2UubGlzdEpvYnMoKTtcbiAgICB9LCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl0pO1xuICB9KTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkdWlWaWV3U2Nyb2xsUHJvdmlkZXIpIHtcbiAgcmV0dXJuICR1aVZpZXdTY3JvbGxQcm92aWRlci51c2VBbmNob3JTY3JvbGwoKTtcbn0pLnJ1bihmdW5jdGlvbigkcm9vdFNjb3BlLCAkc3RhdGUpIHtcbiAgcmV0dXJuICRyb290U2NvcGUuJG9uKCckc3RhdGVDaGFuZ2VTdGFydCcsIGZ1bmN0aW9uKGV2ZW50LCB0b1N0YXRlLCB0b1BhcmFtcywgZnJvbVN0YXRlKSB7XG4gICAgaWYgKHRvU3RhdGUucmVkaXJlY3RUbykge1xuICAgICAgZXZlbnQucHJldmVudERlZmF1bHQoKTtcbiAgICAgIHJldHVybiAkc3RhdGUuZ28odG9TdGF0ZS5yZWRpcmVjdFRvLCB0b1BhcmFtcyk7XG4gICAgfVxuICB9KTtcbn0pLmNvbmZpZyhmdW5jdGlvbigkc3RhdGVQcm92aWRlciwgJHVybFJvdXRlclByb3ZpZGVyKSB7XG4gICRzdGF0ZVByb3ZpZGVyLnN0YXRlKFwib3ZlcnZpZXdcIiwge1xuICAgIHVybDogXCIvb3ZlcnZpZXdcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9vdmVydmlldy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdPdmVydmlld0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInJ1bm5pbmctam9ic1wiLCB7XG4gICAgdXJsOiBcIi9ydW5uaW5nLWpvYnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL3J1bm5pbmctam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdSdW5uaW5nSm9ic0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImNvbXBsZXRlZC1qb2JzXCIsIHtcbiAgICB1cmw6IFwiL2NvbXBsZXRlZC1qb2JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIG1haW46IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9jb21wbGV0ZWQtam9icy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYlwiLCB7XG4gICAgdXJsOiBcIi9qb2JzL3tqb2JpZH1cIixcbiAgICBhYnN0cmFjdDogdHJ1ZSxcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVKb2JDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW5cIiwge1xuICAgIHVybDogXCJcIixcbiAgICByZWRpcmVjdFRvOiBcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4uaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLCB7XG4gICAgdXJsOiBcIlwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnbm9kZS1kZXRhaWxzJzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUtbGlzdC5zdWJ0YXNrcy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4ubWV0cmljc1wiLCB7XG4gICAgdXJsOiBcIi9tZXRyaWNzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0Lm1ldHJpY3MuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbk1ldHJpY3NDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnBsYW4ud2F0ZXJtYXJrc1wiLCB7XG4gICAgdXJsOiBcIi93YXRlcm1hcmtzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LndhdGVybWFya3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IucGxhbi5hY2N1bXVsYXRvcnNcIiwge1xuICAgIHVybDogXCIvYWNjdW11bGF0b3JzXCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdub2RlLWRldGFpbHMnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS1saXN0LmFjY3VtdWxhdG9ycy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JQbGFuQWNjdW11bGF0b3JzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzXCIsIHtcbiAgICB1cmw6IFwiL2NoZWNrcG9pbnRzXCIsXG4gICAgcmVkaXJlY3RUbzogXCJzaW5nbGUtam9iLnBsYW4uY2hlY2twb2ludHMub3ZlcnZpZXdcIixcbiAgICB2aWV3czoge1xuICAgICAgJ25vZGUtZGV0YWlscyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QuY2hlY2twb2ludHMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLm92ZXJ2aWV3XCIsIHtcbiAgICB1cmw6IFwiL292ZXJ2aWV3XCIsXG4gICAgdmlld3M6IHtcbiAgICAgICdjaGVja3BvaW50cy12aWV3Jzoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JzL2pvYi5wbGFuLm5vZGUuY2hlY2twb2ludHMub3ZlcnZpZXcuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLnN1bW1hcnlcIiwge1xuICAgIHVybDogXCIvc3VtbWFyeVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLnN1bW1hcnkuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLmhpc3RvcnlcIiwge1xuICAgIHVybDogXCIvaGlzdG9yeVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLmhpc3RvcnkuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLmNvbmZpZ1wiLCB7XG4gICAgdXJsOiBcIi9jb25maWdcIixcbiAgICB2aWV3czoge1xuICAgICAgJ2NoZWNrcG9pbnRzLXZpZXcnOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnBsYW4ubm9kZS5jaGVja3BvaW50cy5jb25maWcuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnRzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmNoZWNrcG9pbnRzLmRldGFpbHNcIiwge1xuICAgIHVybDogXCIvZGV0YWlscy97Y2hlY2twb2ludElkfVwiLFxuICAgIHZpZXdzOiB7XG4gICAgICAnY2hlY2twb2ludHMtdmlldyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLmNoZWNrcG9pbnRzLmRldGFpbHMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iUGxhbkNoZWNrcG9pbnREZXRhaWxzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5wbGFuLmJhY2twcmVzc3VyZVwiLCB7XG4gICAgdXJsOiBcIi9iYWNrcHJlc3N1cmVcIixcbiAgICB2aWV3czoge1xuICAgICAgJ25vZGUtZGV0YWlscyc6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IucGxhbi5ub2RlLWxpc3QuYmFja3ByZXNzdXJlLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYlBsYW5CYWNrUHJlc3N1cmVDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtam9iLnRpbWVsaW5lXCIsIHtcbiAgICB1cmw6IFwiL3RpbWVsaW5lXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IudGltZWxpbmUuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IudGltZWxpbmUudmVydGV4XCIsIHtcbiAgICB1cmw6IFwiL3t2ZXJ0ZXhJZH1cIixcbiAgICB2aWV3czoge1xuICAgICAgdmVydGV4OiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYnMvam9iLnRpbWVsaW5lLnZlcnRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1qb2IuZXhjZXB0aW9uc1wiLCB7XG4gICAgdXJsOiBcIi9leGNlcHRpb25zXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuZXhjZXB0aW9ucy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JFeGNlcHRpb25zQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic2luZ2xlLWpvYi5jb25maWdcIiwge1xuICAgIHVybDogXCIvY29uZmlnXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9icy9qb2IuY29uZmlnLmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJhbGwtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlcnNcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci9pbmRleC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdBbGxUYXNrTWFuYWdlcnNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtbWFuYWdlclwiLCB7XG4gICAgdXJsOiBcIi90YXNrbWFuYWdlci97dGFza21hbmFnZXJpZH1cIixcbiAgICBhYnN0cmFjdDogdHJ1ZSxcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdTaW5nbGVUYXNrTWFuYWdlckNvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLm1ldHJpY3NcIiwge1xuICAgIHVybDogXCIvbWV0cmljc1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3Rhc2ttYW5hZ2VyL3Rhc2ttYW5hZ2VyLm1ldHJpY3MuaHRtbFwiXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLnN0ZG91dFwiLCB7XG4gICAgdXJsOiBcIi9zdGRvdXRcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5zdGRvdXQuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlVGFza01hbmFnZXJTdGRvdXRDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJzaW5nbGUtbWFuYWdlci5sb2dcIiwge1xuICAgIHVybDogXCIvbG9nXCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvdGFza21hbmFnZXIvdGFza21hbmFnZXIubG9nLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ1NpbmdsZVRhc2tNYW5hZ2VyTG9nc0NvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcInNpbmdsZS1tYW5hZ2VyLmFydGhhc1wiLCB7XG4gICAgdXJsOiBcIi9hcnRoYXNcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy90YXNrbWFuYWdlci90YXNrbWFuYWdlci5hcnRoYXMuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnU2luZ2xlVGFza01hbmFnZXJBcnRoYXNDb250cm9sbGVyJ1xuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyXCIsIHtcbiAgICB1cmw6IFwiL2pvYm1hbmFnZXJcIixcbiAgICB2aWV3czoge1xuICAgICAgbWFpbjoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2luZGV4Lmh0bWxcIlxuICAgICAgfVxuICAgIH1cbiAgfSkuc3RhdGUoXCJqb2JtYW5hZ2VyLmNvbmZpZ1wiLCB7XG4gICAgdXJsOiBcIi9jb25maWdcIixcbiAgICB2aWV3czoge1xuICAgICAgZGV0YWlsczoge1xuICAgICAgICB0ZW1wbGF0ZVVybDogXCJwYXJ0aWFscy9qb2JtYW5hZ2VyL2NvbmZpZy5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6ICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwiam9ibWFuYWdlci5zdGRvdXRcIiwge1xuICAgIHVybDogXCIvc3Rkb3V0XCIsXG4gICAgdmlld3M6IHtcbiAgICAgIGRldGFpbHM6IHtcbiAgICAgICAgdGVtcGxhdGVVcmw6IFwicGFydGlhbHMvam9ibWFuYWdlci9zdGRvdXQuaHRtbFwiLFxuICAgICAgICBjb250cm9sbGVyOiAnSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInXG4gICAgICB9XG4gICAgfVxuICB9KS5zdGF0ZShcImpvYm1hbmFnZXIubG9nXCIsIHtcbiAgICB1cmw6IFwiL2xvZ1wiLFxuICAgIHZpZXdzOiB7XG4gICAgICBkZXRhaWxzOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL2pvYm1hbmFnZXIvbG9nLmh0bWxcIixcbiAgICAgICAgY29udHJvbGxlcjogJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcidcbiAgICAgIH1cbiAgICB9XG4gIH0pLnN0YXRlKFwic3VibWl0XCIsIHtcbiAgICB1cmw6IFwiL3N1Ym1pdFwiLFxuICAgIHZpZXdzOiB7XG4gICAgICBtYWluOiB7XG4gICAgICAgIHRlbXBsYXRlVXJsOiBcInBhcnRpYWxzL3N1Ym1pdC5odG1sXCIsXG4gICAgICAgIGNvbnRyb2xsZXI6IFwiSm9iU3VibWl0Q29udHJvbGxlclwiXG4gICAgICB9XG4gICAgfVxuICB9KTtcbiAgcmV0dXJuICR1cmxSb3V0ZXJQcm92aWRlci5vdGhlcndpc2UoXCIvb3ZlcnZpZXdcIik7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uZGlyZWN0aXZlICdic0xhYmVsJywgKEpvYnNTZXJ2aWNlKSAtPlxyXG4gIHRyYW5zY2x1ZGU6IHRydWVcclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6IFxyXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcclxuICAgIHN0YXR1czogXCJAXCJcclxuXHJcbiAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCJcclxuICBcclxuICBsaW5rOiAoc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSAtPlxyXG4gICAgc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IC0+XHJcbiAgICAgICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ2JwTGFiZWwnLCAoSm9ic1NlcnZpY2UpIC0+XHJcbiAgdHJhbnNjbHVkZTogdHJ1ZVxyXG4gIHJlcGxhY2U6IHRydWVcclxuICBzY29wZTpcclxuICAgIGdldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3M6IFwiJlwiXHJcbiAgICBzdGF0dXM6IFwiQFwiXHJcblxyXG4gIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiXHJcblxyXG4gIGxpbms6IChzY29wZSwgZWxlbWVudCwgYXR0cnMpIC0+XHJcbiAgICBzY29wZS5nZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzID0gLT5cclxuICAgICAgJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVCYWNrUHJlc3N1cmVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAnaW5kaWNhdG9yUHJpbWFyeScsIChKb2JzU2VydmljZSkgLT5cclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6IFxyXG4gICAgZ2V0TGFiZWxDbGFzczogXCImXCJcclxuICAgIHN0YXR1czogJ0AnXHJcblxyXG4gIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiXHJcbiAgXHJcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cclxuICAgIHNjb3BlLmdldExhYmVsQ2xhc3MgPSAtPlxyXG4gICAgICAnZmEgZmEtY2lyY2xlIGluZGljYXRvciBpbmRpY2F0b3ItJyArIEpvYnNTZXJ2aWNlLnRyYW5zbGF0ZUxhYmVsU3RhdGUoYXR0cnMuc3RhdHVzKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uZGlyZWN0aXZlICd0YWJsZVByb3BlcnR5JywgLT5cclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6XHJcbiAgICB2YWx1ZTogJz0nXHJcblxyXG4gIHRlbXBsYXRlOiBcIjx0ZCB0aXRsZT1cXFwie3t2YWx1ZSB8fCAnTm9uZSd9fVxcXCI+e3t2YWx1ZSB8fCAnTm9uZSd9fTwvdGQ+XCJcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuZGlyZWN0aXZlKCdic0xhYmVsJywgZnVuY3Rpb24oSm9ic1NlcnZpY2UpIHtcbiAgcmV0dXJuIHtcbiAgICB0cmFuc2NsdWRlOiB0cnVlLFxuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiBcIkBcIlxuICAgIH0sXG4gICAgdGVtcGxhdGU6IFwiPHNwYW4gdGl0bGU9J3t7c3RhdHVzfX0nIG5nLWNsYXNzPSdnZXRMYWJlbENsYXNzKCknPjxuZy10cmFuc2NsdWRlPjwvbmctdHJhbnNjbHVkZT48L3NwYW4+XCIsXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW1lbnQsIGF0dHJzKSB7XG4gICAgICByZXR1cm4gc2NvcGUuZ2V0TGFiZWxDbGFzcyA9IGZ1bmN0aW9uKCkge1xuICAgICAgICByZXR1cm4gJ2xhYmVsIGxhYmVsLScgKyBKb2JzU2VydmljZS50cmFuc2xhdGVMYWJlbFN0YXRlKGF0dHJzLnN0YXR1cyk7XG4gICAgICB9O1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnYnBMYWJlbCcsIGZ1bmN0aW9uKEpvYnNTZXJ2aWNlKSB7XG4gIHJldHVybiB7XG4gICAgdHJhbnNjbHVkZTogdHJ1ZSxcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICBnZXRCYWNrUHJlc3N1cmVMYWJlbENsYXNzOiBcIiZcIixcbiAgICAgIHN0YXR1czogXCJAXCJcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxzcGFuIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0QmFja1ByZXNzdXJlTGFiZWxDbGFzcygpJz48bmctdHJhbnNjbHVkZT48L25nLXRyYW5zY2x1ZGU+PC9zcGFuPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldEJhY2tQcmVzc3VyZUxhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdsYWJlbCBsYWJlbC0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlQmFja1ByZXNzdXJlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ2luZGljYXRvclByaW1hcnknLCBmdW5jdGlvbihKb2JzU2VydmljZSkge1xuICByZXR1cm4ge1xuICAgIHJlcGxhY2U6IHRydWUsXG4gICAgc2NvcGU6IHtcbiAgICAgIGdldExhYmVsQ2xhc3M6IFwiJlwiLFxuICAgICAgc3RhdHVzOiAnQCdcbiAgICB9LFxuICAgIHRlbXBsYXRlOiBcIjxpIHRpdGxlPSd7e3N0YXR1c319JyBuZy1jbGFzcz0nZ2V0TGFiZWxDbGFzcygpJyAvPlwiLFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgcmV0dXJuIHNjb3BlLmdldExhYmVsQ2xhc3MgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgcmV0dXJuICdmYSBmYS1jaXJjbGUgaW5kaWNhdG9yIGluZGljYXRvci0nICsgSm9ic1NlcnZpY2UudHJhbnNsYXRlTGFiZWxTdGF0ZShhdHRycy5zdGF0dXMpO1xuICAgICAgfTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RhYmxlUHJvcGVydHknLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICByZXBsYWNlOiB0cnVlLFxuICAgIHNjb3BlOiB7XG4gICAgICB2YWx1ZTogJz0nXG4gICAgfSxcbiAgICB0ZW1wbGF0ZTogXCI8dGQgdGl0bGU9XFxcInt7dmFsdWUgfHwgJ05vbmUnfX1cXFwiPnt7dmFsdWUgfHwgJ05vbmUnfX08L3RkPlwiXG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmZpbHRlciBcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCAoYW5ndWxhck1vbWVudENvbmZpZykgLT5cclxuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIgPSAodmFsdWUsIGZvcm1hdCwgZHVyYXRpb25Gb3JtYXQpIC0+XHJcbiAgICByZXR1cm4gXCJcIiAgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxyXG5cclxuICAgIG1vbWVudC5kdXJhdGlvbih2YWx1ZSwgZm9ybWF0KS5mb3JtYXQoZHVyYXRpb25Gb3JtYXQsIHsgdHJpbTogZmFsc2UgfSlcclxuXHJcbiAgYW1EdXJhdGlvbkZvcm1hdEV4dGVuZGVkRmlsdGVyLiRzdGF0ZWZ1bCA9IGFuZ3VsYXJNb21lbnRDb25maWcuc3RhdGVmdWxGaWx0ZXJzXHJcblxyXG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlclxyXG5cclxuLmZpbHRlciBcImh1bWFuaXplRHVyYXRpb25cIiwgLT5cclxuICAodmFsdWUsIHNob3J0KSAtPlxyXG4gICAgcmV0dXJuIFwiXCIgaWYgdHlwZW9mIHZhbHVlIGlzIFwidW5kZWZpbmVkXCIgb3IgdmFsdWUgaXMgbnVsbFxyXG4gICAgbXMgPSB2YWx1ZSAlIDEwMDBcclxuICAgIHggPSBNYXRoLmZsb29yKHZhbHVlIC8gMTAwMClcclxuICAgIHNlY29uZHMgPSB4ICUgNjBcclxuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MClcclxuICAgIG1pbnV0ZXMgPSB4ICUgNjBcclxuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MClcclxuICAgIGhvdXJzID0geCAlIDI0XHJcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gMjQpXHJcbiAgICBkYXlzID0geFxyXG4gICAgaWYgZGF5cyA9PSAwXHJcbiAgICAgIGlmIGhvdXJzID09IDBcclxuICAgICAgICBpZiBtaW51dGVzID09IDBcclxuICAgICAgICAgIGlmIHNlY29uZHMgPT0gMFxyXG4gICAgICAgICAgICByZXR1cm4gbXMgKyBcIm1zXCJcclxuICAgICAgICAgIGVsc2VcclxuICAgICAgICAgICAgcmV0dXJuIHNlY29uZHMgKyBcInMgXCJcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICByZXR1cm4gbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIlxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgaWYgc2hvcnQgdGhlbiByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtXCIgZWxzZSByZXR1cm4gaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiXHJcbiAgICBlbHNlXHJcbiAgICAgIGlmIHNob3J0IHRoZW4gcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaFwiIGVsc2UgcmV0dXJuIGRheXMgKyBcImQgXCIgKyBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCJcclxuXHJcbi5maWx0ZXIgXCJsaW1pdFwiLCAtPlxyXG4gICh0ZXh0KSAtPlxyXG4gICAgaWYgKHRleHQubGVuZ3RoID4gNzMpXHJcbiAgICAgIHRleHQgPSB0ZXh0LnN1YnN0cmluZygwLCAzNSkgKyBcIi4uLlwiICsgdGV4dC5zdWJzdHJpbmcodGV4dC5sZW5ndGggLSAzNSwgdGV4dC5sZW5ndGgpXHJcbiAgICB0ZXh0XHJcblxyXG4uZmlsdGVyIFwiaHVtYW5pemVUZXh0XCIsIC0+XHJcbiAgKHRleHQpIC0+XHJcbiAgICAjIFRPRE86IGV4dGVuZC4uLiBhIGxvdFxyXG4gICAgaWYgdGV4dCB0aGVuIHRleHQucmVwbGFjZSgvJmd0Oy9nLCBcIj5cIikucmVwbGFjZSgvPGJyXFwvPi9nLFwiXCIpIGVsc2UgJydcclxuXHJcbi5maWx0ZXIgXCJodW1hbml6ZUJ5dGVzXCIsIC0+XHJcbiAgKGJ5dGVzKSAtPlxyXG4gICAgdW5pdHMgPSBbXCJCXCIsIFwiS0JcIiwgXCJNQlwiLCBcIkdCXCIsIFwiVEJcIiwgXCJQQlwiLCBcIkVCXCJdXHJcbiAgICBjb252ZXJ0ZXIgPSAodmFsdWUsIHBvd2VyKSAtPlxyXG4gICAgICBiYXNlID0gTWF0aC5wb3coMTAyNCwgcG93ZXIpXHJcbiAgICAgIGlmIHZhbHVlIDwgYmFzZVxyXG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b0ZpeGVkKDIpICsgXCIgXCIgKyB1bml0c1twb3dlcl1cclxuICAgICAgZWxzZSBpZiB2YWx1ZSA8IGJhc2UgKiAxMDAwXHJcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvUHJlY2lzaW9uKDMpICsgXCIgXCIgKyB1bml0c1twb3dlcl1cclxuICAgICAgZWxzZVxyXG4gICAgICAgIHJldHVybiBjb252ZXJ0ZXIodmFsdWUsIHBvd2VyICsgMSlcclxuICAgIHJldHVybiBcIlwiIGlmIHR5cGVvZiBieXRlcyBpcyBcInVuZGVmaW5lZFwiIG9yIGJ5dGVzIGlzIG51bGxcclxuICAgIGlmIGJ5dGVzIDwgMTAwMCB0aGVuIGJ5dGVzICsgXCIgQlwiIGVsc2UgY29udmVydGVyKGJ5dGVzLCAxKVxyXG5cclxuLmZpbHRlciBcInRvTG9jYWxlU3RyaW5nXCIsIC0+XHJcbiAgKHRleHQpIC0+IHRleHQudG9Mb2NhbGVTdHJpbmcoKVxyXG5cclxuLmZpbHRlciBcInRvVXBwZXJDYXNlXCIsIC0+XHJcbiAgKHRleHQpIC0+IHRleHQudG9VcHBlckNhc2UoKVxyXG5cclxuLmZpbHRlciBcInBlcmNlbnRhZ2VcIiwgLT5cclxuICAobnVtYmVyKSAtPiAobnVtYmVyICogMTAwKS50b0ZpeGVkKDApICsgJyUnXHJcblxyXG4uZmlsdGVyIFwiaHVtYW5pemVXYXRlcm1hcmtcIiwgKHdhdGVybWFya3NDb25maWcpIC0+XHJcbiAgKHZhbHVlKSAtPlxyXG4gICAgaWYgaXNOYU4odmFsdWUpIHx8IHZhbHVlIDw9IHdhdGVybWFya3NDb25maWcubm9XYXRlcm1hcmtcclxuICAgICAgcmV0dXJuICdObyBXYXRlcm1hcmsnXHJcbiAgICBlbHNlXHJcbiAgICAgIHJldHVybiB2YWx1ZVxyXG5cclxuLmZpbHRlciBcImluY3JlbWVudFwiLCAtPlxyXG4gIChudW1iZXIpIC0+XHJcbiAgICBwYXJzZUludChudW1iZXIpICsgMVxyXG5cclxuLmZpbHRlciBcImh1bWFuaXplQ2hhcnROdW1lcmljXCIsIFsnaHVtYW5pemVCeXRlc0ZpbHRlcicsICdodW1hbml6ZUR1cmF0aW9uRmlsdGVyJywgKGh1bWFuaXplQnl0ZXNGaWx0ZXIsIGh1bWFuaXplRHVyYXRpb25GaWx0ZXIpLT5cclxuICAodmFsdWUsIG1ldHJpYyktPlxyXG4gICAgcmV0dXJuX3ZhbCA9ICcnXHJcbiAgICBpZiB2YWx1ZSAhPSBudWxsXHJcbiAgICAgIGlmIC9ieXRlcy9pLnRlc3QobWV0cmljLmlkKSAmJiAvcGVyc2Vjb25kL2kudGVzdChtZXRyaWMuaWQpXHJcbiAgICAgICAgcmV0dXJuX3ZhbCA9IGh1bWFuaXplQnl0ZXNGaWx0ZXIodmFsdWUpICsgJyAvIHMnXHJcbiAgICAgIGVsc2UgaWYgL2J5dGVzL2kudGVzdChtZXRyaWMuaWQpXHJcbiAgICAgICAgcmV0dXJuX3ZhbCA9IGh1bWFuaXplQnl0ZXNGaWx0ZXIodmFsdWUpXHJcbiAgICAgIGVsc2UgaWYgL3BlcnNlY29uZC9pLnRlc3QobWV0cmljLmlkKVxyXG4gICAgICAgIHJldHVybl92YWwgPSB2YWx1ZSArICcgLyBzJ1xyXG4gICAgICBlbHNlIGlmIC90aW1lL2kudGVzdChtZXRyaWMuaWQpIHx8IC9sYXRlbmN5L2kudGVzdChtZXRyaWMuaWQpXHJcbiAgICAgICAgcmV0dXJuX3ZhbCA9IGh1bWFuaXplRHVyYXRpb25GaWx0ZXIodmFsdWUsIHRydWUpXHJcbiAgICAgIGVsc2VcclxuICAgICAgICByZXR1cm5fdmFsID0gdmFsdWVcclxuICAgIHJldHVybiByZXR1cm5fdmFsXHJcbl1cclxuXHJcbi5maWx0ZXIgXCJodW1hbml6ZUNoYXJ0TnVtZXJpY1RpdGxlXCIsIFsnaHVtYW5pemVEdXJhdGlvbkZpbHRlcicsIChodW1hbml6ZUR1cmF0aW9uRmlsdGVyKS0+XHJcbiAgKHZhbHVlLCBtZXRyaWMpLT5cclxuICAgIHJldHVybl92YWwgPSAnJ1xyXG4gICAgaWYgdmFsdWUgIT0gbnVsbFxyXG4gICAgICBpZiAvYnl0ZXMvaS50ZXN0KG1ldHJpYy5pZCkgJiYgL3BlcnNlY29uZC9pLnRlc3QobWV0cmljLmlkKVxyXG4gICAgICAgIHJldHVybl92YWwgPSB2YWx1ZSArICcgQnl0ZXMgLyBzJ1xyXG4gICAgICBlbHNlIGlmIC9ieXRlcy9pLnRlc3QobWV0cmljLmlkKVxyXG4gICAgICAgIHJldHVybl92YWwgPSB2YWx1ZSArICcgQnl0ZXMnXHJcbiAgICAgIGVsc2UgaWYgL3BlcnNlY29uZC9pLnRlc3QobWV0cmljLmlkKVxyXG4gICAgICAgIHJldHVybl92YWwgPSB2YWx1ZSArICcgLyBzJ1xyXG4gICAgICBlbHNlIGlmIC90aW1lL2kudGVzdChtZXRyaWMuaWQpIHx8IC9sYXRlbmN5L2kudGVzdChtZXRyaWMuaWQpXHJcbiAgICAgICAgcmV0dXJuX3ZhbCA9IGh1bWFuaXplRHVyYXRpb25GaWx0ZXIodmFsdWUsIGZhbHNlKVxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgcmV0dXJuX3ZhbCA9IHZhbHVlXHJcbiAgICByZXR1cm4gcmV0dXJuX3ZhbFxyXG5dXHJcblxyXG4uZmlsdGVyIFwic2VhcmNoTWV0cmljc1wiLCAtPlxyXG4gIChhdmFpbGFibGVNZXRyaWNzLCBxdWVyeSktPlxyXG4gICAgcXVlcnlSZWdleCA9IG5ldyBSZWdFeHAocXVlcnksIFwiZ2lcIilcclxuICAgIHJldHVybiAobWV0cmljIGZvciBtZXRyaWMgaW4gYXZhaWxhYmxlTWV0cmljcyB3aGVuIG1ldHJpYy5pZC5tYXRjaChxdWVyeVJlZ2V4KSlcclxuXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmZpbHRlcihcImFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZFwiLCBmdW5jdGlvbihhbmd1bGFyTW9tZW50Q29uZmlnKSB7XG4gIHZhciBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG4gIGFtRHVyYXRpb25Gb3JtYXRFeHRlbmRlZEZpbHRlciA9IGZ1bmN0aW9uKHZhbHVlLCBmb3JtYXQsIGR1cmF0aW9uRm9ybWF0KSB7XG4gICAgaWYgKHR5cGVvZiB2YWx1ZSA9PT0gXCJ1bmRlZmluZWRcIiB8fCB2YWx1ZSA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIHJldHVybiBtb21lbnQuZHVyYXRpb24odmFsdWUsIGZvcm1hdCkuZm9ybWF0KGR1cmF0aW9uRm9ybWF0LCB7XG4gICAgICB0cmltOiBmYWxzZVxuICAgIH0pO1xuICB9O1xuICBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXIuJHN0YXRlZnVsID0gYW5ndWxhck1vbWVudENvbmZpZy5zdGF0ZWZ1bEZpbHRlcnM7XG4gIHJldHVybiBhbUR1cmF0aW9uRm9ybWF0RXh0ZW5kZWRGaWx0ZXI7XG59KS5maWx0ZXIoXCJodW1hbml6ZUR1cmF0aW9uXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odmFsdWUsIHNob3J0KSB7XG4gICAgdmFyIGRheXMsIGhvdXJzLCBtaW51dGVzLCBtcywgc2Vjb25kcywgeDtcbiAgICBpZiAodHlwZW9mIHZhbHVlID09PSBcInVuZGVmaW5lZFwiIHx8IHZhbHVlID09PSBudWxsKSB7XG4gICAgICByZXR1cm4gXCJcIjtcbiAgICB9XG4gICAgbXMgPSB2YWx1ZSAlIDEwMDA7XG4gICAgeCA9IE1hdGguZmxvb3IodmFsdWUgLyAxMDAwKTtcbiAgICBzZWNvbmRzID0geCAlIDYwO1xuICAgIHggPSBNYXRoLmZsb29yKHggLyA2MCk7XG4gICAgbWludXRlcyA9IHggJSA2MDtcbiAgICB4ID0gTWF0aC5mbG9vcih4IC8gNjApO1xuICAgIGhvdXJzID0geCAlIDI0O1xuICAgIHggPSBNYXRoLmZsb29yKHggLyAyNCk7XG4gICAgZGF5cyA9IHg7XG4gICAgaWYgKGRheXMgPT09IDApIHtcbiAgICAgIGlmIChob3VycyA9PT0gMCkge1xuICAgICAgICBpZiAobWludXRlcyA9PT0gMCkge1xuICAgICAgICAgIGlmIChzZWNvbmRzID09PSAwKSB7XG4gICAgICAgICAgICByZXR1cm4gbXMgKyBcIm1zXCI7XG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIHJldHVybiBzZWNvbmRzICsgXCJzIFwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm4gbWludXRlcyArIFwibSBcIiArIHNlY29uZHMgKyBcInNcIjtcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgaWYgKHNob3J0KSB7XG4gICAgICAgICAgcmV0dXJuIGhvdXJzICsgXCJoIFwiICsgbWludXRlcyArIFwibVwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiBob3VycyArIFwiaCBcIiArIG1pbnV0ZXMgKyBcIm0gXCIgKyBzZWNvbmRzICsgXCJzXCI7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHNob3J0KSB7XG4gICAgICAgIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImhcIjtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiBkYXlzICsgXCJkIFwiICsgaG91cnMgKyBcImggXCIgKyBtaW51dGVzICsgXCJtIFwiICsgc2Vjb25kcyArIFwic1wiO1xuICAgICAgfVxuICAgIH1cbiAgfTtcbn0pLmZpbHRlcihcImxpbWl0XCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIGlmICh0ZXh0Lmxlbmd0aCA+IDczKSB7XG4gICAgICB0ZXh0ID0gdGV4dC5zdWJzdHJpbmcoMCwgMzUpICsgXCIuLi5cIiArIHRleHQuc3Vic3RyaW5nKHRleHQubGVuZ3RoIC0gMzUsIHRleHQubGVuZ3RoKTtcbiAgICB9XG4gICAgcmV0dXJuIHRleHQ7XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZVRleHRcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgaWYgKHRleHQpIHtcbiAgICAgIHJldHVybiB0ZXh0LnJlcGxhY2UoLyZndDsvZywgXCI+XCIpLnJlcGxhY2UoLzxiclxcLz4vZywgXCJcIik7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAnJztcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZUJ5dGVzXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24oYnl0ZXMpIHtcbiAgICB2YXIgY29udmVydGVyLCB1bml0cztcbiAgICB1bml0cyA9IFtcIkJcIiwgXCJLQlwiLCBcIk1CXCIsIFwiR0JcIiwgXCJUQlwiLCBcIlBCXCIsIFwiRUJcIl07XG4gICAgY29udmVydGVyID0gZnVuY3Rpb24odmFsdWUsIHBvd2VyKSB7XG4gICAgICB2YXIgYmFzZTtcbiAgICAgIGJhc2UgPSBNYXRoLnBvdygxMDI0LCBwb3dlcik7XG4gICAgICBpZiAodmFsdWUgPCBiYXNlKSB7XG4gICAgICAgIHJldHVybiAodmFsdWUgLyBiYXNlKS50b0ZpeGVkKDIpICsgXCIgXCIgKyB1bml0c1twb3dlcl07XG4gICAgICB9IGVsc2UgaWYgKHZhbHVlIDwgYmFzZSAqIDEwMDApIHtcbiAgICAgICAgcmV0dXJuICh2YWx1ZSAvIGJhc2UpLnRvUHJlY2lzaW9uKDMpICsgXCIgXCIgKyB1bml0c1twb3dlcl07XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gY29udmVydGVyKHZhbHVlLCBwb3dlciArIDEpO1xuICAgICAgfVxuICAgIH07XG4gICAgaWYgKHR5cGVvZiBieXRlcyA9PT0gXCJ1bmRlZmluZWRcIiB8fCBieXRlcyA9PT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFwiXCI7XG4gICAgfVxuICAgIGlmIChieXRlcyA8IDEwMDApIHtcbiAgICAgIHJldHVybiBieXRlcyArIFwiIEJcIjtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnZlcnRlcihieXRlcywgMSk7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwidG9Mb2NhbGVTdHJpbmdcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbih0ZXh0KSB7XG4gICAgcmV0dXJuIHRleHQudG9Mb2NhbGVTdHJpbmcoKTtcbiAgfTtcbn0pLmZpbHRlcihcInRvVXBwZXJDYXNlXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24odGV4dCkge1xuICAgIHJldHVybiB0ZXh0LnRvVXBwZXJDYXNlKCk7XG4gIH07XG59KS5maWx0ZXIoXCJwZXJjZW50YWdlXCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24obnVtYmVyKSB7XG4gICAgcmV0dXJuIChudW1iZXIgKiAxMDApLnRvRml4ZWQoMCkgKyAnJSc7XG4gIH07XG59KS5maWx0ZXIoXCJodW1hbml6ZVdhdGVybWFya1wiLCBmdW5jdGlvbih3YXRlcm1hcmtzQ29uZmlnKSB7XG4gIHJldHVybiBmdW5jdGlvbih2YWx1ZSkge1xuICAgIGlmIChpc05hTih2YWx1ZSkgfHwgdmFsdWUgPD0gd2F0ZXJtYXJrc0NvbmZpZy5ub1dhdGVybWFyaykge1xuICAgICAgcmV0dXJuICdObyBXYXRlcm1hcmsnO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gdmFsdWU7XG4gICAgfVxuICB9O1xufSkuZmlsdGVyKFwiaW5jcmVtZW50XCIsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4gZnVuY3Rpb24obnVtYmVyKSB7XG4gICAgcmV0dXJuIHBhcnNlSW50KG51bWJlcikgKyAxO1xuICB9O1xufSkuZmlsdGVyKFwiaHVtYW5pemVDaGFydE51bWVyaWNcIiwgW1xuICAnaHVtYW5pemVCeXRlc0ZpbHRlcicsICdodW1hbml6ZUR1cmF0aW9uRmlsdGVyJywgZnVuY3Rpb24oaHVtYW5pemVCeXRlc0ZpbHRlciwgaHVtYW5pemVEdXJhdGlvbkZpbHRlcikge1xuICAgIHJldHVybiBmdW5jdGlvbih2YWx1ZSwgbWV0cmljKSB7XG4gICAgICB2YXIgcmV0dXJuX3ZhbDtcbiAgICAgIHJldHVybl92YWwgPSAnJztcbiAgICAgIGlmICh2YWx1ZSAhPT0gbnVsbCkge1xuICAgICAgICBpZiAoL2J5dGVzL2kudGVzdChtZXRyaWMuaWQpICYmIC9wZXJzZWNvbmQvaS50ZXN0KG1ldHJpYy5pZCkpIHtcbiAgICAgICAgICByZXR1cm5fdmFsID0gaHVtYW5pemVCeXRlc0ZpbHRlcih2YWx1ZSkgKyAnIC8gcyc7XG4gICAgICAgIH0gZWxzZSBpZiAoL2J5dGVzL2kudGVzdChtZXRyaWMuaWQpKSB7XG4gICAgICAgICAgcmV0dXJuX3ZhbCA9IGh1bWFuaXplQnl0ZXNGaWx0ZXIodmFsdWUpO1xuICAgICAgICB9IGVsc2UgaWYgKC9wZXJzZWNvbmQvaS50ZXN0KG1ldHJpYy5pZCkpIHtcbiAgICAgICAgICByZXR1cm5fdmFsID0gdmFsdWUgKyAnIC8gcyc7XG4gICAgICAgIH0gZWxzZSBpZiAoL3RpbWUvaS50ZXN0KG1ldHJpYy5pZCkgfHwgL2xhdGVuY3kvaS50ZXN0KG1ldHJpYy5pZCkpIHtcbiAgICAgICAgICByZXR1cm5fdmFsID0gaHVtYW5pemVEdXJhdGlvbkZpbHRlcih2YWx1ZSwgdHJ1ZSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuX3ZhbCA9IHZhbHVlO1xuICAgICAgICB9XG4gICAgICB9XG4gICAgICByZXR1cm4gcmV0dXJuX3ZhbDtcbiAgICB9O1xuICB9XG5dKS5maWx0ZXIoXCJodW1hbml6ZUNoYXJ0TnVtZXJpY1RpdGxlXCIsIFtcbiAgJ2h1bWFuaXplRHVyYXRpb25GaWx0ZXInLCBmdW5jdGlvbihodW1hbml6ZUR1cmF0aW9uRmlsdGVyKSB7XG4gICAgcmV0dXJuIGZ1bmN0aW9uKHZhbHVlLCBtZXRyaWMpIHtcbiAgICAgIHZhciByZXR1cm5fdmFsO1xuICAgICAgcmV0dXJuX3ZhbCA9ICcnO1xuICAgICAgaWYgKHZhbHVlICE9PSBudWxsKSB7XG4gICAgICAgIGlmICgvYnl0ZXMvaS50ZXN0KG1ldHJpYy5pZCkgJiYgL3BlcnNlY29uZC9pLnRlc3QobWV0cmljLmlkKSkge1xuICAgICAgICAgIHJldHVybl92YWwgPSB2YWx1ZSArICcgQnl0ZXMgLyBzJztcbiAgICAgICAgfSBlbHNlIGlmICgvYnl0ZXMvaS50ZXN0KG1ldHJpYy5pZCkpIHtcbiAgICAgICAgICByZXR1cm5fdmFsID0gdmFsdWUgKyAnIEJ5dGVzJztcbiAgICAgICAgfSBlbHNlIGlmICgvcGVyc2Vjb25kL2kudGVzdChtZXRyaWMuaWQpKSB7XG4gICAgICAgICAgcmV0dXJuX3ZhbCA9IHZhbHVlICsgJyAvIHMnO1xuICAgICAgICB9IGVsc2UgaWYgKC90aW1lL2kudGVzdChtZXRyaWMuaWQpIHx8IC9sYXRlbmN5L2kudGVzdChtZXRyaWMuaWQpKSB7XG4gICAgICAgICAgcmV0dXJuX3ZhbCA9IGh1bWFuaXplRHVyYXRpb25GaWx0ZXIodmFsdWUsIGZhbHNlKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICByZXR1cm5fdmFsID0gdmFsdWU7XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHJldHVybiByZXR1cm5fdmFsO1xuICAgIH07XG4gIH1cbl0pLmZpbHRlcihcInNlYXJjaE1ldHJpY3NcIiwgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihhdmFpbGFibGVNZXRyaWNzLCBxdWVyeSkge1xuICAgIHZhciBtZXRyaWMsIHF1ZXJ5UmVnZXg7XG4gICAgcXVlcnlSZWdleCA9IG5ldyBSZWdFeHAocXVlcnksIFwiZ2lcIik7XG4gICAgcmV0dXJuIChmdW5jdGlvbigpIHtcbiAgICAgIHZhciBpLCBsZW4sIHJlc3VsdHM7XG4gICAgICByZXN1bHRzID0gW107XG4gICAgICBmb3IgKGkgPSAwLCBsZW4gPSBhdmFpbGFibGVNZXRyaWNzLmxlbmd0aDsgaSA8IGxlbjsgaSsrKSB7XG4gICAgICAgIG1ldHJpYyA9IGF2YWlsYWJsZU1ldHJpY3NbaV07XG4gICAgICAgIGlmIChtZXRyaWMuaWQubWF0Y2gocXVlcnlSZWdleCkpIHtcbiAgICAgICAgICByZXN1bHRzLnB1c2gobWV0cmljKTtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIHJlc3VsdHM7XG4gICAgfSkoKTtcbiAgfTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnTWFpblNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cclxuICBAbG9hZENvbmZpZyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIlxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuXHJcbiAgQFxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5zZXJ2aWNlKCdNYWluU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkQ29uZmlnID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJjb25maWdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5jb250cm9sbGVyICdKb2JNYW5hZ2VyQ29uZmlnQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSAtPlxyXG4gIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlLmxvYWRDb25maWcoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgaWYgISRzY29wZS5qb2JtYW5hZ2VyP1xyXG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9XHJcbiAgICAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhXHJcblxyXG4uY29udHJvbGxlciAnSm9iTWFuYWdlckxvZ3NDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSAtPlxyXG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XHJcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cclxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydsb2cnXSA9IGRhdGFcclxuXHJcbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxyXG4gICAgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlLmxvYWRMb2dzKCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYk1hbmFnZXJTdGRvdXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UpIC0+XHJcbiAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICBpZiAhJHNjb3BlLmpvYm1hbmFnZXI/XHJcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge31cclxuICAgICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGFcclxuXHJcbiAgJHNjb3BlLnJlbG9hZERhdGEgPSAoKSAtPlxyXG4gICAgSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGFcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignSm9iTWFuYWdlckNvbmZpZ0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJDb25maWdTZXJ2aWNlKSB7XG4gIHJldHVybiBKb2JNYW5hZ2VyQ29uZmlnU2VydmljZS5sb2FkQ29uZmlnKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgaWYgKCRzY29wZS5qb2JtYW5hZ2VyID09IG51bGwpIHtcbiAgICAgICRzY29wZS5qb2JtYW5hZ2VyID0ge307XG4gICAgfVxuICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnY29uZmlnJ10gPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYk1hbmFnZXJMb2dzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgSm9iTWFuYWdlckxvZ3NTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJMb2dzU2VydmljZS5sb2FkTG9ncygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIGlmICgkc2NvcGUuam9ibWFuYWdlciA9PSBudWxsKSB7XG4gICAgICAkc2NvcGUuam9ibWFuYWdlciA9IHt9O1xuICAgIH1cbiAgICByZXR1cm4gJHNjb3BlLmpvYm1hbmFnZXJbJ2xvZyddID0gZGF0YTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUucmVsb2FkRGF0YSA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JNYW5hZ2VyTG9nc1NlcnZpY2UubG9hZExvZ3MoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuam9ibWFuYWdlclsnbG9nJ10gPSBkYXRhO1xuICAgIH0pO1xuICB9O1xufSkuY29udHJvbGxlcignSm9iTWFuYWdlclN0ZG91dENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlKSB7XG4gIEpvYk1hbmFnZXJTdGRvdXRTZXJ2aWNlLmxvYWRTdGRvdXQoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICBpZiAoJHNjb3BlLmpvYm1hbmFnZXIgPT0gbnVsbCkge1xuICAgICAgJHNjb3BlLmpvYm1hbmFnZXIgPSB7fTtcbiAgICB9XG4gICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UubG9hZFN0ZG91dCgpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5qb2JtYW5hZ2VyWydzdGRvdXQnXSA9IGRhdGE7XG4gICAgfSk7XG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLnNlcnZpY2UgJ0pvYk1hbmFnZXJDb25maWdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgY29uZmlnID0ge31cclxuXHJcbiAgQGxvYWRDb25maWcgPSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9ibWFuYWdlci9jb25maWdcIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgY29uZmlnID0gZGF0YVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcblxyXG4uc2VydmljZSAnSm9iTWFuYWdlckxvZ3NTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgbG9ncyA9IHt9XHJcblxyXG4gIEBsb2FkTG9ncyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL2xvZ1wiKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBsb2dzID0gZGF0YVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcblxyXG4uc2VydmljZSAnSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UnLCAoJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkgLT5cclxuICBzdGRvdXQgPSB7fVxyXG5cclxuICBAbG9hZFN0ZG91dCA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JtYW5hZ2VyL3N0ZG91dFwiKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBzdGRvdXQgPSBkYXRhXHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBcclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnSm9iTWFuYWdlckNvbmZpZ1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBjb25maWc7XG4gIGNvbmZpZyA9IHt9O1xuICB0aGlzLmxvYWRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGNvbmZpZyA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KS5zZXJ2aWNlKCdKb2JNYW5hZ2VyTG9nc1NlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBsb2dzO1xuICBsb2dzID0ge307XG4gIHRoaXMubG9hZExvZ3MgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvbG9nXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIGxvZ3MgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnSm9iTWFuYWdlclN0ZG91dFNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHZhciBzdGRvdXQ7XG4gIHN0ZG91dCA9IHt9O1xuICB0aGlzLmxvYWRTdGRvdXQgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYm1hbmFnZXIvc3Rkb3V0XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHN0ZG91dCA9IGRhdGE7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxyXG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdydW5uaW5nJylcclxuXHJcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcclxuXHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdDb21wbGV0ZWRKb2JzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuam9iT2JzZXJ2ZXIgPSAtPlxyXG4gICAgJHNjb3BlLmpvYnMgPSBKb2JzU2VydmljZS5nZXRKb2JzKCdmaW5pc2hlZCcpXHJcblxyXG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKVxyXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cclxuICAgIEpvYnNTZXJ2aWNlLnVuUmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcblxyXG4gICRzY29wZS5qb2JPYnNlcnZlcigpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnU2luZ2xlSm9iQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgTWV0cmljc1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwsICRxLCB3YXRlcm1hcmtzQ29uZmlnKSAtPlxyXG4gICRzY29wZS5qb2JpZCA9ICRzdGF0ZVBhcmFtcy5qb2JpZFxyXG4gICRzY29wZS5qb2IgPSBudWxsXHJcbiAgJHNjb3BlLnBsYW4gPSBudWxsXHJcbiAgJHNjb3BlLndhdGVybWFya3MgPSB7fVxyXG4gICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcclxuICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0cyA9IHt9XHJcblxyXG4gIHJlZnJlc2hlciA9ICRpbnRlcnZhbCAtPlxyXG4gICAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5qb2IgPSBkYXRhXHJcbiAgICAgICRzY29wZS4kYnJvYWRjYXN0ICdyZWxvYWQnXHJcblxyXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXHJcblxyXG4gICRzY29wZS4kb24gJyRkZXN0cm95JywgLT5cclxuICAgICRzY29wZS5qb2IgPSBudWxsXHJcbiAgICAkc2NvcGUucGxhbiA9IG51bGxcclxuICAgICRzY29wZS53YXRlcm1hcmtzID0ge31cclxuICAgICRzY29wZS52ZXJ0aWNlcyA9IG51bGxcclxuICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzID0gbnVsbFxyXG5cclxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaGVyKVxyXG5cclxuICAkc2NvcGUuY2FuY2VsSm9iID0gKGNhbmNlbEV2ZW50KSAtPlxyXG4gICAgYW5ndWxhci5lbGVtZW50KGNhbmNlbEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnQ2FuY2VsbGluZy4uLicpXHJcbiAgICBKb2JzU2VydmljZS5jYW5jZWxKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICB7fVxyXG5cclxuICAkc2NvcGUuc3RvcEpvYiA9IChzdG9wRXZlbnQpIC0+XHJcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3RvcEV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiYnRuXCIpLnJlbW92ZUNsYXNzKFwiYnRuLWRlZmF1bHRcIikuaHRtbCgnU3RvcHBpbmcuLi4nKVxyXG4gICAgSm9ic1NlcnZpY2Uuc3RvcEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgIHt9XHJcblxyXG4gIEpvYnNTZXJ2aWNlLmxvYWRKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgJHNjb3BlLmpvYiA9IGRhdGFcclxuICAgICRzY29wZS52ZXJ0aWNlcyA9IGRhdGEudmVydGljZXNcclxuICAgICRzY29wZS5wbGFuID0gZGF0YS5wbGFuXHJcbiAgICBNZXRyaWNzU2VydmljZS5zZXR1cE1ldHJpY3MoJHN0YXRlUGFyYW1zLmpvYmlkLCBkYXRhLnZlcnRpY2VzKVxyXG5cclxuICAjIEFzeW5jaHJvbm91c2x5IHJlcXVlc3RzIHRoZSB3YXRlcm1hcmsgbWV0cmljcyBmb3IgdGhlIGdpdmVuIG5vZGVzLiBUaGVcclxuICAjIHJldHVybmVkIG9iamVjdCBoYXMgdGhlIGZvbGxvd2luZyBzdHJ1Y3R1cmU6XHJcbiAgI1xyXG4gICMge1xyXG4gICMgICAgXCI8bm9kZUlkPlwiOiB7XHJcbiAgIyAgICAgICAgICBcImxvd1dhdGVybWFya1wiOiA8bG93V2F0ZXJtYXJrPlxyXG4gICMgICAgICAgICAgXCJ3YXRlcm1hcmtzXCI6IHtcclxuICAjICAgICAgICAgICAgICAgMDogPHdhdGVybWFyayBmb3Igc3VidGFzayAwPlxyXG4gICMgICAgICAgICAgICAgICAuLi5cclxuICAjICAgICAgICAgICAgICAgbjogPHdhdGVybWFyayBmb3Igc3VidGFzayBuPlxyXG4gICMgICAgICAgICAgICB9XHJcbiAgIyAgICAgICB9XHJcbiAgIyB9XHJcbiAgI1xyXG4gICMgSWYgbm8gd2F0ZXJtYXJrIGlzIGF2YWlsYWJsZSwgbG93V2F0ZXJtYXJrIHdpbGwgYmUgTmFOIGFuZFxyXG4gICMgdGhlIHdhdGVybWFya3Mgd2lsbCBiZSBlbXB0eS5cclxuICBnZXRXYXRlcm1hcmtzID0gKG5vZGVzKSAtPlxyXG4gICAgIyBSZXF1ZXN0cyB0aGUgd2F0ZXJtYXJrcyBmb3IgYSBzaW5nbGUgdmVydGV4LiBUcmlnZ2VycyBhIHJlcXVlc3RcclxuICAgICMgdG8gdGhlIE1ldHJpY3Mgc2VydmljZS5cclxuICAgIHJlcXVlc3RXYXRlcm1hcmtGb3JOb2RlID0gKG5vZGUpID0+XHJcbiAgICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICAgamlkID0gJHNjb3BlLmpvYi5qaWRcclxuXHJcbiAgICAgICMgUmVxdWVzdCBtZXRyaWNzIGZvciBlYWNoIHN1YnRhc2tcclxuICAgICAgbWV0cmljSWRzID0gKGkgKyBcIi5jdXJyZW50SW5wdXRXYXRlcm1hcmtcIiBmb3IgaSBpbiBbMC4ubm9kZS5wYXJhbGxlbGlzbSAtIDFdKVxyXG4gICAgICBNZXRyaWNzU2VydmljZS5nZXRNZXRyaWNzKGppZCwgbm9kZS5pZCwgbWV0cmljSWRzKS50aGVuIChtZXRyaWNzKSAtPlxyXG4gICAgICAgIG1pblZhbHVlID0gTmFOXHJcbiAgICAgICAgd2F0ZXJtYXJrcyA9IHt9XHJcblxyXG4gICAgICAgIGZvciBrZXksIHZhbHVlIG9mIG1ldHJpY3MudmFsdWVzXHJcbiAgICAgICAgICBzdWJ0YXNrSW5kZXggPSBrZXkucmVwbGFjZSgnLmN1cnJlbnRJbnB1dFdhdGVybWFyaycsICcnKVxyXG4gICAgICAgICAgd2F0ZXJtYXJrc1tzdWJ0YXNrSW5kZXhdID0gdmFsdWVcclxuXHJcbiAgICAgICAgICBpZiAoaXNOYU4obWluVmFsdWUpIHx8IHZhbHVlIDwgbWluVmFsdWUpXHJcbiAgICAgICAgICAgIG1pblZhbHVlID0gdmFsdWVcclxuXHJcbiAgICAgICAgaWYgKCFpc05hTihtaW5WYWx1ZSkgJiYgbWluVmFsdWUgPiB3YXRlcm1hcmtzQ29uZmlnLm5vV2F0ZXJtYXJrKVxyXG4gICAgICAgICAgbG93V2F0ZXJtYXJrID0gbWluVmFsdWVcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICAjIE5hTiBpbmRpY2F0ZXMgbm8gd2F0ZXJtYXJrIGF2YWlsYWJsZVxyXG4gICAgICAgICAgbG93V2F0ZXJtYXJrID0gTmFOXHJcblxyXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoe1wibG93V2F0ZXJtYXJrXCI6IGxvd1dhdGVybWFyaywgXCJ3YXRlcm1hcmtzXCI6IHdhdGVybWFya3N9KVxyXG5cclxuICAgICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG4gICAgd2F0ZXJtYXJrcyA9IHt9XHJcblxyXG4gICAgIyBSZXF1ZXN0IHdhdGVybWFya3MgZm9yIGVhY2ggbm9kZSBhbmQgdXBkYXRlIHdhdGVybWFya3NcclxuICAgIGxlbiA9IG5vZGVzLmxlbmd0aFxyXG4gICAgYW5ndWxhci5mb3JFYWNoIG5vZGVzLCAobm9kZSwgaW5kZXgpID0+XHJcbiAgICAgIG5vZGVJZCA9IG5vZGUuaWRcclxuICAgICAgcmVxdWVzdFdhdGVybWFya0Zvck5vZGUobm9kZSkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgICB3YXRlcm1hcmtzW25vZGVJZF0gPSBkYXRhXHJcbiAgICAgICAgaWYgKGluZGV4ID49IGxlbiAtIDEpXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHdhdGVybWFya3MpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAjIFJldHVybnMgdHJ1ZSBpZiB0aGUgbG93V2F0ZXJtYXJrIGlzICE9IE5hTlxyXG4gICRzY29wZS5oYXNXYXRlcm1hcmsgPSAobm9kZWlkKSAtPlxyXG4gICAgJHNjb3BlLndhdGVybWFya3Nbbm9kZWlkXSAmJiAhaXNOYU4oJHNjb3BlLndhdGVybWFya3Nbbm9kZWlkXVtcImxvd1dhdGVybWFya1wiXSlcclxuXHJcbiAgJHNjb3BlLiR3YXRjaCAncGxhbicsIChuZXdQbGFuKSAtPlxyXG4gICAgaWYgbmV3UGxhblxyXG4gICAgICBnZXRXYXRlcm1hcmtzKG5ld1BsYW4ubm9kZXMpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgJHNjb3BlLndhdGVybWFya3MgPSBkYXRhXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsICgpIC0+XHJcbiAgICBpZiAkc2NvcGUucGxhblxyXG4gICAgICBnZXRXYXRlcm1hcmtzKCRzY29wZS5wbGFuLm5vZGVzKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS53YXRlcm1hcmtzID0gZGF0YVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5Db250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsICR3aW5kb3csIEpvYnNTZXJ2aWNlKSAtPlxyXG4gICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlXHJcbiAgJHNjb3BlLnN0YXRlTGlzdCA9IEpvYnNTZXJ2aWNlLnN0YXRlTGlzdCgpXHJcblxyXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcclxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcclxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxyXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxyXG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsXHJcblxyXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAncmVsb2FkJ1xyXG4gICAgICAkc2NvcGUuJGJyb2FkY2FzdCAnbm9kZTpjaGFuZ2UnLCAkc2NvcGUubm9kZWlkXHJcblxyXG4gICAgZWxzZVxyXG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxyXG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2VcclxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcclxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxyXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxyXG4gICAgICAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsXHJcblxyXG4gICRzY29wZS5kZWFjdGl2YXRlTm9kZSA9IC0+XHJcbiAgICAkc2NvcGUubm9kZWlkID0gbnVsbFxyXG4gICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlXHJcbiAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxyXG4gICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxyXG4gICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGxcclxuICAgICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGxcclxuXHJcbiAgJHNjb3BlLnRvZ2dsZUZvbGQgPSAtPlxyXG4gICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9ICEkc2NvcGUubm9kZVVuZm9sZGVkXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUGxhblN1YnRhc2tzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxyXG4gICRzY29wZS5hZ2dyZWdhdGUgPSBmYWxzZVxyXG5cclxuICBnZXRTdWJ0YXNrcyA9IC0+XHJcbiAgICBpZiAkc2NvcGUuYWdncmVnYXRlXHJcbiAgICAgIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS50YXNrbWFuYWdlcnMgPSBkYXRhXHJcbiAgICBlbHNlXHJcbiAgICAgIEpvYnNTZXJ2aWNlLmdldFN1YnRhc2tzKCRzY29wZS5ub2RlaWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICAgJHNjb3BlLnN1YnRhc2tzID0gZGF0YVxyXG5cclxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguc3QpXHJcbiAgICBnZXRTdWJ0YXNrcygpXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgIGdldFN1YnRhc2tzKCkgaWYgJHNjb3BlLm5vZGVpZFxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5BY2N1bXVsYXRvcnNDb250cm9sbGVyJywgKCRzY29wZSwgSm9ic1NlcnZpY2UpIC0+XHJcbiAgZ2V0QWNjdW11bGF0b3JzID0gLT5cclxuICAgIEpvYnNTZXJ2aWNlLmdldEFjY3VtdWxhdG9ycygkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gZGF0YS5tYWluXHJcbiAgICAgICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrc1xyXG5cclxuICBpZiAkc2NvcGUubm9kZWlkIGFuZCAoISRzY29wZS52ZXJ0ZXggb3IgISRzY29wZS52ZXJ0ZXguYWNjdW11bGF0b3JzKVxyXG4gICAgZ2V0QWNjdW11bGF0b3JzKClcclxuXHJcbiAgJHNjb3BlLiRvbiAncmVsb2FkJywgKGV2ZW50KSAtPlxyXG4gICAgZ2V0QWNjdW11bGF0b3JzKCkgaWYgJHNjb3BlLm5vZGVpZFxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5DaGVja3BvaW50c0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XHJcbiAgIyBVcGRhdGVkIGJ5IHRoZSBkZXRhaWxzIGhhbmRsZXIgZm9yIHRoZSBzdWIgY2hlY2twb2ludHMgbmF2IGJhci5cclxuICAkc2NvcGUuY2hlY2twb2ludERldGFpbHMgPSB7fVxyXG4gICRzY29wZS5jaGVja3BvaW50RGV0YWlscy5pZCA9IC0xXHJcblxyXG4gICMgUmVxdWVzdCB0aGUgY29uZmlnIG9uY2UgKGl0J3Mgc3RhdGljKVxyXG4gIEpvYnNTZXJ2aWNlLmdldENoZWNrcG9pbnRDb25maWcoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgJHNjb3BlLmNoZWNrcG9pbnRDb25maWcgPSBkYXRhXHJcblxyXG4gICMgR2VuZXJhbCBzdGF0cyBsaWtlIGNvdW50cywgaGlzdG9yeSwgZXRjLlxyXG4gIGdldEdlbmVyYWxDaGVja3BvaW50U3RhdHMgPSAtPlxyXG4gICAgSm9ic1NlcnZpY2UuZ2V0Q2hlY2twb2ludFN0YXRzKCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgaWYgKGRhdGEgIT0gbnVsbClcclxuICAgICAgICAkc2NvcGUuY2hlY2twb2ludFN0YXRzID0gZGF0YVxyXG5cclxuICAjIFRyaWdnZXIgcmVxdWVzdFxyXG4gIGdldEdlbmVyYWxDaGVja3BvaW50U3RhdHMoKVxyXG5cclxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XHJcbiAgICAjIFJldHJpZ2dlciByZXF1ZXN0XHJcbiAgICBnZXRHZW5lcmFsQ2hlY2twb2ludFN0YXRzKClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JQbGFuQ2hlY2twb2ludERldGFpbHNDb250cm9sbGVyJywgKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSAtPlxyXG4gICRzY29wZS5zdWJ0YXNrRGV0YWlscyA9IHt9XHJcbiAgJHNjb3BlLmNoZWNrcG9pbnREZXRhaWxzLmlkID0gJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZFxyXG5cclxuICAjIERldGFpbGVkIHN0YXRzIGZvciBhIHNpbmdsZSBjaGVja3BvaW50XHJcbiAgZ2V0Q2hlY2twb2ludERldGFpbHMgPSAoY2hlY2twb2ludElkKSAtPlxyXG4gICAgSm9ic1NlcnZpY2UuZ2V0Q2hlY2twb2ludERldGFpbHMoY2hlY2twb2ludElkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICBpZiAoZGF0YSAhPSBudWxsKVxyXG4gICAgICAgICRzY29wZS5jaGVja3BvaW50ID0gZGF0YVxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgJHNjb3BlLnVua25vd25fY2hlY2twb2ludCA9IHRydWVcclxuXHJcbiAgZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzID0gKGNoZWNrcG9pbnRJZCwgdmVydGV4SWQpIC0+XHJcbiAgICBKb2JzU2VydmljZS5nZXRDaGVja3BvaW50U3VidGFza0RldGFpbHMoY2hlY2twb2ludElkLCB2ZXJ0ZXhJZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgaWYgKGRhdGEgIT0gbnVsbClcclxuICAgICAgICAkc2NvcGUuc3VidGFza0RldGFpbHNbdmVydGV4SWRdID0gZGF0YVxyXG5cclxuICBnZXRDaGVja3BvaW50RGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkKVxyXG5cclxuICBpZiAoJHNjb3BlLm5vZGVpZClcclxuICAgIGdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkLCAkc2NvcGUubm9kZWlkKVxyXG5cclxuICAkc2NvcGUuJG9uICdyZWxvYWQnLCAoZXZlbnQpIC0+XHJcbiAgICBnZXRDaGVja3BvaW50RGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkKVxyXG5cclxuICAgIGlmICgkc2NvcGUubm9kZWlkKVxyXG4gICAgICBnZXRDaGVja3BvaW50U3VidGFza0RldGFpbHMoJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZCwgJHNjb3BlLm5vZGVpZClcclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJHNjb3BlLmNoZWNrcG9pbnREZXRhaWxzLmlkID0gLTFcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JQbGFuQmFja1ByZXNzdXJlQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlKSAtPlxyXG4gIGdldE9wZXJhdG9yQmFja1ByZXNzdXJlID0gLT5cclxuICAgICRzY29wZS5ub3cgPSBEYXRlLm5vdygpXHJcblxyXG4gICAgaWYgJHNjb3BlLm5vZGVpZFxyXG4gICAgICBKb2JzU2VydmljZS5nZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgkc2NvcGUubm9kZWlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgICRzY29wZS5iYWNrUHJlc3N1cmVPcGVyYXRvclN0YXRzWyRzY29wZS5ub2RlaWRdID0gZGF0YVxyXG5cclxuICBnZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgpXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgIGdldE9wZXJhdG9yQmFja1ByZXNzdXJlKClcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5jb250cm9sbGVyICdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XHJcbiAgZ2V0VmVydGV4ID0gLT5cclxuICAgIEpvYnNTZXJ2aWNlLmdldFZlcnRleCgkc3RhdGVQYXJhbXMudmVydGV4SWQpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBkYXRhXHJcblxyXG4gIGdldFZlcnRleCgpXHJcblxyXG4gICRzY29wZS4kb24gJ3JlbG9hZCcsIChldmVudCkgLT5cclxuICAgIGdldFZlcnRleCgpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCAoJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIC0+XHJcbiAgSm9ic1NlcnZpY2UubG9hZEV4Y2VwdGlvbnMoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgJHNjb3BlLmV4Y2VwdGlvbnMgPSBkYXRhXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uY29udHJvbGxlciAnSm9iUHJvcGVydGllc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBKb2JzU2VydmljZSkgLT5cclxuICAkc2NvcGUuY2hhbmdlTm9kZSA9IChub2RlaWQpIC0+XHJcbiAgICBpZiBub2RlaWQgIT0gJHNjb3BlLm5vZGVpZFxyXG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkXHJcblxyXG4gICAgICBKb2JzU2VydmljZS5nZXROb2RlKG5vZGVpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgICAkc2NvcGUubm9kZSA9IGRhdGFcclxuXHJcbiAgICBlbHNlXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgICAgICRzY29wZS5ub2RlID0gbnVsbFxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmNvbnRyb2xsZXIgJ0pvYlBsYW5NZXRyaWNzQ29udHJvbGxlcicsICgkc2NvcGUsIEpvYnNTZXJ2aWNlLCBNZXRyaWNzU2VydmljZSkgLT5cclxuICAkc2NvcGUuZHJhZ2dpbmcgPSBmYWxzZVxyXG4gICRzY29wZS53aW5kb3cgPSBNZXRyaWNzU2VydmljZS5nZXRXaW5kb3coKVxyXG4gICRzY29wZS5hdmFpbGFibGVNZXRyaWNzID0gbnVsbFxyXG5cclxuICAkc2NvcGUuJG9uICckZGVzdHJveScsIC0+XHJcbiAgICBNZXRyaWNzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoKVxyXG5cclxuICBsb2FkTWV0cmljcyA9IC0+XHJcbiAgICBKb2JzU2VydmljZS5nZXRWZXJ0ZXgoJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLnZlcnRleCA9IGRhdGFcclxuXHJcbiAgICBNZXRyaWNzU2VydmljZS5nZXRBdmFpbGFibGVNZXRyaWNzKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgJHNjb3BlLmF2YWlsYWJsZU1ldHJpY3MgPSBkYXRhLnNvcnQoYWxwaGFiZXRpY2FsU29ydEJ5SWQpXHJcbiAgICAgICRzY29wZS5tZXRyaWNzID0gTWV0cmljc1NlcnZpY2UuZ2V0TWV0cmljc1NldHVwKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCkubmFtZXNcclxuXHJcbiAgICAgIE1ldHJpY3NTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCAoZGF0YSkgLT5cclxuICAgICAgICAkc2NvcGUuJGJyb2FkY2FzdCBcIm1ldHJpY3M6ZGF0YTp1cGRhdGVcIiwgZGF0YS50aW1lc3RhbXAsIGRhdGEudmFsdWVzXHJcbiAgICAgIClcclxuXHJcbiAgYWxwaGFiZXRpY2FsU29ydEJ5SWQgPSAoYSwgYikgLT5cclxuICAgIEEgPSBhLmlkLnRvTG93ZXJDYXNlKClcclxuICAgIEIgPSBiLmlkLnRvTG93ZXJDYXNlKClcclxuICAgIGlmIEEgPCBCXHJcbiAgICAgIHJldHVybiAtMVxyXG4gICAgZWxzZSBpZiBBID4gQlxyXG4gICAgICByZXR1cm4gMVxyXG4gICAgZWxzZVxyXG4gICAgICByZXR1cm4gMFxyXG5cclxuICAkc2NvcGUuZHJvcHBlZCA9IChldmVudCwgaW5kZXgsIGl0ZW0sIGV4dGVybmFsLCB0eXBlKSAtPlxyXG5cclxuICAgIE1ldHJpY3NTZXJ2aWNlLm9yZGVyTWV0cmljcygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIGl0ZW0sIGluZGV4KVxyXG4gICAgJHNjb3BlLiRicm9hZGNhc3QgXCJtZXRyaWNzOnJlZnJlc2hcIiwgaXRlbVxyXG4gICAgbG9hZE1ldHJpY3MoKVxyXG4gICAgZmFsc2VcclxuXHJcbiAgJHNjb3BlLmRyYWdTdGFydCA9IC0+XHJcbiAgICAkc2NvcGUuZHJhZ2dpbmcgPSB0cnVlXHJcblxyXG4gICRzY29wZS5kcmFnRW5kID0gLT5cclxuICAgICRzY29wZS5kcmFnZ2luZyA9IGZhbHNlXHJcblxyXG4gICRzY29wZS5hZGRNZXRyaWMgPSAobWV0cmljKSAtPlxyXG4gICAgTWV0cmljc1NlcnZpY2UuYWRkTWV0cmljKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgbWV0cmljLmlkKVxyXG4gICAgbG9hZE1ldHJpY3MoKVxyXG5cclxuICAkc2NvcGUucmVtb3ZlTWV0cmljID0gKG1ldHJpYykgLT5cclxuICAgIE1ldHJpY3NTZXJ2aWNlLnJlbW92ZU1ldHJpYygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYylcclxuICAgIGxvYWRNZXRyaWNzKClcclxuXHJcbiAgJHNjb3BlLnNldE1ldHJpY1NpemUgPSAobWV0cmljLCBzaXplKSAtPlxyXG4gICAgTWV0cmljc1NlcnZpY2Uuc2V0TWV0cmljU2l6ZSgkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYywgc2l6ZSlcclxuICAgIGxvYWRNZXRyaWNzKClcclxuXHJcbiAgJHNjb3BlLnNldE1ldHJpY1ZpZXcgPSAobWV0cmljLCB2aWV3KSAtPlxyXG4gICAgTWV0cmljc1NlcnZpY2Uuc2V0TWV0cmljVmlldygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYywgdmlldylcclxuICAgIGxvYWRNZXRyaWNzKClcclxuXHJcbiAgJHNjb3BlLmdldFZhbHVlcyA9IChtZXRyaWMpIC0+XHJcbiAgICBNZXRyaWNzU2VydmljZS5nZXRWYWx1ZXMoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBtZXRyaWMpXHJcblxyXG4gICRzY29wZS4kb24gJ25vZGU6Y2hhbmdlJywgKGV2ZW50LCBub2RlaWQpIC0+XHJcbiAgICBsb2FkTWV0cmljcygpIGlmICEkc2NvcGUuZHJhZ2dpbmdcclxuXHJcbiAgbG9hZE1ldHJpY3MoKSBpZiAkc2NvcGUubm9kZWlkXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ1J1bm5pbmdKb2JzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUuam9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ0NvbXBsZXRlZEpvYnNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5qb2JzID0gSm9ic1NlcnZpY2UuZ2V0Sm9icygnZmluaXNoZWQnKTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgfSk7XG4gIHJldHVybiAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZUpvYkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSwgTWV0cmljc1NlcnZpY2UsICRyb290U2NvcGUsIGZsaW5rQ29uZmlnLCAkaW50ZXJ2YWwsICRxLCB3YXRlcm1hcmtzQ29uZmlnKSB7XG4gIHZhciBnZXRXYXRlcm1hcmtzLCByZWZyZXNoZXI7XG4gICRzY29wZS5qb2JpZCA9ICRzdGF0ZVBhcmFtcy5qb2JpZDtcbiAgJHNjb3BlLmpvYiA9IG51bGw7XG4gICRzY29wZS5wbGFuID0gbnVsbDtcbiAgJHNjb3BlLndhdGVybWFya3MgPSB7fTtcbiAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHMgPSB7fTtcbiAgcmVmcmVzaGVyID0gJGludGVydmFsKGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS5sb2FkSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuam9iID0gZGF0YTtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgncmVsb2FkJyk7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLmpvYiA9IG51bGw7XG4gICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICRzY29wZS53YXRlcm1hcmtzID0ge307XG4gICAgJHNjb3BlLnZlcnRpY2VzID0gbnVsbDtcbiAgICAkc2NvcGUuYmFja1ByZXNzdXJlT3BlcmF0b3JTdGF0cyA9IG51bGw7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaGVyKTtcbiAgfSk7XG4gICRzY29wZS5jYW5jZWxKb2IgPSBmdW5jdGlvbihjYW5jZWxFdmVudCkge1xuICAgIGFuZ3VsYXIuZWxlbWVudChjYW5jZWxFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ0NhbmNlbGxpbmcuLi4nKTtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuY2FuY2VsSm9iKCRzdGF0ZVBhcmFtcy5qb2JpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4ge307XG4gICAgfSk7XG4gIH07XG4gICRzY29wZS5zdG9wSm9iID0gZnVuY3Rpb24oc3RvcEV2ZW50KSB7XG4gICAgYW5ndWxhci5lbGVtZW50KHN0b3BFdmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImJ0blwiKS5yZW1vdmVDbGFzcyhcImJ0bi1kZWZhdWx0XCIpLmh0bWwoJ1N0b3BwaW5nLi4uJyk7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLnN0b3BKb2IoJHN0YXRlUGFyYW1zLmpvYmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiB7fTtcbiAgICB9KTtcbiAgfTtcbiAgSm9ic1NlcnZpY2UubG9hZEpvYigkc3RhdGVQYXJhbXMuam9iaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICRzY29wZS5qb2IgPSBkYXRhO1xuICAgICRzY29wZS52ZXJ0aWNlcyA9IGRhdGEudmVydGljZXM7XG4gICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW47XG4gICAgcmV0dXJuIE1ldHJpY3NTZXJ2aWNlLnNldHVwTWV0cmljcygkc3RhdGVQYXJhbXMuam9iaWQsIGRhdGEudmVydGljZXMpO1xuICB9KTtcbiAgZ2V0V2F0ZXJtYXJrcyA9IGZ1bmN0aW9uKG5vZGVzKSB7XG4gICAgdmFyIGRlZmVycmVkLCBsZW4sIHJlcXVlc3RXYXRlcm1hcmtGb3JOb2RlLCB3YXRlcm1hcmtzO1xuICAgIHJlcXVlc3RXYXRlcm1hcmtGb3JOb2RlID0gKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24obm9kZSkge1xuICAgICAgICB2YXIgZGVmZXJyZWQsIGksIGppZCwgbWV0cmljSWRzO1xuICAgICAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgICAgIGppZCA9ICRzY29wZS5qb2IuamlkO1xuICAgICAgICBtZXRyaWNJZHMgPSAoZnVuY3Rpb24oKSB7XG4gICAgICAgICAgdmFyIGosIHJlZiwgcmVzdWx0cztcbiAgICAgICAgICByZXN1bHRzID0gW107XG4gICAgICAgICAgZm9yIChpID0gaiA9IDAsIHJlZiA9IG5vZGUucGFyYWxsZWxpc20gLSAxOyAwIDw9IHJlZiA/IGogPD0gcmVmIDogaiA+PSByZWY7IGkgPSAwIDw9IHJlZiA/ICsraiA6IC0taikge1xuICAgICAgICAgICAgcmVzdWx0cy5wdXNoKGkgKyBcIi5jdXJyZW50SW5wdXRXYXRlcm1hcmtcIik7XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiByZXN1bHRzO1xuICAgICAgICB9KSgpO1xuICAgICAgICBNZXRyaWNzU2VydmljZS5nZXRNZXRyaWNzKGppZCwgbm9kZS5pZCwgbWV0cmljSWRzKS50aGVuKGZ1bmN0aW9uKG1ldHJpY3MpIHtcbiAgICAgICAgICB2YXIga2V5LCBsb3dXYXRlcm1hcmssIG1pblZhbHVlLCByZWYsIHN1YnRhc2tJbmRleCwgdmFsdWUsIHdhdGVybWFya3M7XG4gICAgICAgICAgbWluVmFsdWUgPSAwLzA7XG4gICAgICAgICAgd2F0ZXJtYXJrcyA9IHt9O1xuICAgICAgICAgIHJlZiA9IG1ldHJpY3MudmFsdWVzO1xuICAgICAgICAgIGZvciAoa2V5IGluIHJlZikge1xuICAgICAgICAgICAgdmFsdWUgPSByZWZba2V5XTtcbiAgICAgICAgICAgIHN1YnRhc2tJbmRleCA9IGtleS5yZXBsYWNlKCcuY3VycmVudElucHV0V2F0ZXJtYXJrJywgJycpO1xuICAgICAgICAgICAgd2F0ZXJtYXJrc1tzdWJ0YXNrSW5kZXhdID0gdmFsdWU7XG4gICAgICAgICAgICBpZiAoaXNOYU4obWluVmFsdWUpIHx8IHZhbHVlIDwgbWluVmFsdWUpIHtcbiAgICAgICAgICAgICAgbWluVmFsdWUgPSB2YWx1ZTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKCFpc05hTihtaW5WYWx1ZSkgJiYgbWluVmFsdWUgPiB3YXRlcm1hcmtzQ29uZmlnLm5vV2F0ZXJtYXJrKSB7XG4gICAgICAgICAgICBsb3dXYXRlcm1hcmsgPSBtaW5WYWx1ZTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgbG93V2F0ZXJtYXJrID0gMC8wO1xuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICBcImxvd1dhdGVybWFya1wiOiBsb3dXYXRlcm1hcmssXG4gICAgICAgICAgICBcIndhdGVybWFya3NcIjogd2F0ZXJtYXJrc1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICB3YXRlcm1hcmtzID0ge307XG4gICAgbGVuID0gbm9kZXMubGVuZ3RoO1xuICAgIGFuZ3VsYXIuZm9yRWFjaChub2RlcywgKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24obm9kZSwgaW5kZXgpIHtcbiAgICAgICAgdmFyIG5vZGVJZDtcbiAgICAgICAgbm9kZUlkID0gbm9kZS5pZDtcbiAgICAgICAgcmV0dXJuIHJlcXVlc3RXYXRlcm1hcmtGb3JOb2RlKG5vZGUpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHdhdGVybWFya3Nbbm9kZUlkXSA9IGRhdGE7XG4gICAgICAgICAgaWYgKGluZGV4ID49IGxlbiAtIDEpIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKHdhdGVybWFya3MpO1xuICAgICAgICAgIH1cbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgJHNjb3BlLmhhc1dhdGVybWFyayA9IGZ1bmN0aW9uKG5vZGVpZCkge1xuICAgIHJldHVybiAkc2NvcGUud2F0ZXJtYXJrc1tub2RlaWRdICYmICFpc05hTigkc2NvcGUud2F0ZXJtYXJrc1tub2RlaWRdW1wibG93V2F0ZXJtYXJrXCJdKTtcbiAgfTtcbiAgJHNjb3BlLiR3YXRjaCgncGxhbicsIGZ1bmN0aW9uKG5ld1BsYW4pIHtcbiAgICBpZiAobmV3UGxhbikge1xuICAgICAgcmV0dXJuIGdldFdhdGVybWFya3MobmV3UGxhbi5ub2RlcykudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUud2F0ZXJtYXJrcyA9IGRhdGE7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oKSB7XG4gICAgaWYgKCRzY29wZS5wbGFuKSB7XG4gICAgICByZXR1cm4gZ2V0V2F0ZXJtYXJrcygkc2NvcGUucGxhbi5ub2RlcykudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUud2F0ZXJtYXJrcyA9IGRhdGE7XG4gICAgICB9KTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCAkd2luZG93LCBKb2JzU2VydmljZSkge1xuICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAkc2NvcGUuc3RhdGVMaXN0ID0gSm9ic1NlcnZpY2Uuc3RhdGVMaXN0KCk7XG4gICRzY29wZS5jaGFuZ2VOb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgaWYgKG5vZGVpZCAhPT0gJHNjb3BlLm5vZGVpZCkge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZDtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgICAgJHNjb3BlLm9wZXJhdG9yQ2hlY2twb2ludFN0YXRzID0gbnVsbDtcbiAgICAgICRzY29wZS4kYnJvYWRjYXN0KCdyZWxvYWQnKTtcbiAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdCgnbm9kZTpjaGFuZ2UnLCAkc2NvcGUubm9kZWlkKTtcbiAgICB9IGVsc2Uge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUub3BlcmF0b3JDaGVja3BvaW50U3RhdHMgPSBudWxsO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmRlYWN0aXZhdGVOb2RlID0gZnVuY3Rpb24oKSB7XG4gICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgJHNjb3BlLm5vZGVVbmZvbGRlZCA9IGZhbHNlO1xuICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgcmV0dXJuICRzY29wZS5vcGVyYXRvckNoZWNrcG9pbnRTdGF0cyA9IG51bGw7XG4gIH07XG4gIHJldHVybiAkc2NvcGUudG9nZ2xlRm9sZCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUubm9kZVVuZm9sZGVkID0gISRzY29wZS5ub2RlVW5mb2xkZWQ7XG4gIH07XG59KS5jb250cm9sbGVyKCdKb2JQbGFuU3VidGFza3NDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICB2YXIgZ2V0U3VidGFza3M7XG4gICRzY29wZS5hZ2dyZWdhdGUgPSBmYWxzZTtcbiAgZ2V0U3VidGFza3MgPSBmdW5jdGlvbigpIHtcbiAgICBpZiAoJHNjb3BlLmFnZ3JlZ2F0ZSkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldFRhc2tNYW5hZ2Vycygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS50YXNrbWFuYWdlcnMgPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRTdWJ0YXNrcygkc2NvcGUubm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrcyA9IGRhdGE7XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG4gIGlmICgkc2NvcGUubm9kZWlkICYmICghJHNjb3BlLnZlcnRleCB8fCAhJHNjb3BlLnZlcnRleC5zdCkpIHtcbiAgICBnZXRTdWJ0YXNrcygpO1xuICB9XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgICByZXR1cm4gZ2V0U3VidGFza3MoKTtcbiAgICB9XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkFjY3VtdWxhdG9yc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIHZhciBnZXRBY2N1bXVsYXRvcnM7XG4gIGdldEFjY3VtdWxhdG9ycyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JzU2VydmljZS5nZXRBY2N1bXVsYXRvcnMoJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gZGF0YS5tYWluO1xuICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrQWNjdW11bGF0b3JzID0gZGF0YS5zdWJ0YXNrcztcbiAgICB9KTtcbiAgfTtcbiAgaWYgKCRzY29wZS5ub2RlaWQgJiYgKCEkc2NvcGUudmVydGV4IHx8ICEkc2NvcGUudmVydGV4LmFjY3VtdWxhdG9ycykpIHtcbiAgICBnZXRBY2N1bXVsYXRvcnMoKTtcbiAgfVxuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIGdldEFjY3VtdWxhdG9ycygpO1xuICAgIH1cbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JQbGFuQ2hlY2twb2ludHNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGUsICRzdGF0ZVBhcmFtcywgSm9ic1NlcnZpY2UpIHtcbiAgdmFyIGdldEdlbmVyYWxDaGVja3BvaW50U3RhdHM7XG4gICRzY29wZS5jaGVja3BvaW50RGV0YWlscyA9IHt9O1xuICAkc2NvcGUuY2hlY2twb2ludERldGFpbHMuaWQgPSAtMTtcbiAgSm9ic1NlcnZpY2UuZ2V0Q2hlY2twb2ludENvbmZpZygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUuY2hlY2twb2ludENvbmZpZyA9IGRhdGE7XG4gIH0pO1xuICBnZXRHZW5lcmFsQ2hlY2twb2ludFN0YXRzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldENoZWNrcG9pbnRTdGF0cygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgaWYgKGRhdGEgIT09IG51bGwpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5jaGVja3BvaW50U3RhdHMgPSBkYXRhO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICBnZXRHZW5lcmFsQ2hlY2twb2ludFN0YXRzKCk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCdyZWxvYWQnLCBmdW5jdGlvbihldmVudCkge1xuICAgIHJldHVybiBnZXRHZW5lcmFsQ2hlY2twb2ludFN0YXRzKCk7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkNoZWNrcG9pbnREZXRhaWxzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlLCAkc3RhdGVQYXJhbXMsIEpvYnNTZXJ2aWNlKSB7XG4gIHZhciBnZXRDaGVja3BvaW50RGV0YWlscywgZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzO1xuICAkc2NvcGUuc3VidGFza0RldGFpbHMgPSB7fTtcbiAgJHNjb3BlLmNoZWNrcG9pbnREZXRhaWxzLmlkID0gJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZDtcbiAgZ2V0Q2hlY2twb2ludERldGFpbHMgPSBmdW5jdGlvbihjaGVja3BvaW50SWQpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0Q2hlY2twb2ludERldGFpbHMoY2hlY2twb2ludElkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIGlmIChkYXRhICE9PSBudWxsKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuY2hlY2twb2ludCA9IGRhdGE7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICByZXR1cm4gJHNjb3BlLnVua25vd25fY2hlY2twb2ludCA9IHRydWU7XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIGdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscyA9IGZ1bmN0aW9uKGNoZWNrcG9pbnRJZCwgdmVydGV4SWQpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzKGNoZWNrcG9pbnRJZCwgdmVydGV4SWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgaWYgKGRhdGEgIT09IG51bGwpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5zdWJ0YXNrRGV0YWlsc1t2ZXJ0ZXhJZF0gPSBkYXRhO1xuICAgICAgfVxuICAgIH0pO1xuICB9O1xuICBnZXRDaGVja3BvaW50RGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkKTtcbiAgaWYgKCRzY29wZS5ub2RlaWQpIHtcbiAgICBnZXRDaGVja3BvaW50U3VidGFza0RldGFpbHMoJHN0YXRlUGFyYW1zLmNoZWNrcG9pbnRJZCwgJHNjb3BlLm5vZGVpZCk7XG4gIH1cbiAgJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICBnZXRDaGVja3BvaW50RGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIGdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscygkc3RhdGVQYXJhbXMuY2hlY2twb2ludElkLCAkc2NvcGUubm9kZWlkKTtcbiAgICB9XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmNoZWNrcG9pbnREZXRhaWxzLmlkID0gLTE7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iUGxhbkJhY2tQcmVzc3VyZUNvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYnNTZXJ2aWNlKSB7XG4gIHZhciBnZXRPcGVyYXRvckJhY2tQcmVzc3VyZTtcbiAgZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUubm93ID0gRGF0ZS5ub3coKTtcbiAgICBpZiAoJHNjb3BlLm5vZGVpZCkge1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE9wZXJhdG9yQmFja1ByZXNzdXJlKCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJHNjb3BlLmJhY2tQcmVzc3VyZU9wZXJhdG9yU3RhdHNbJHNjb3BlLm5vZGVpZF0gPSBkYXRhO1xuICAgICAgfSk7XG4gICAgfVxuICB9O1xuICBnZXRPcGVyYXRvckJhY2tQcmVzc3VyZSgpO1xuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICByZXR1cm4gZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUoKTtcbiAgfSk7XG59KS5jb250cm9sbGVyKCdKb2JUaW1lbGluZVZlcnRleENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICB2YXIgZ2V0VmVydGV4O1xuICBnZXRWZXJ0ZXggPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzdGF0ZVBhcmFtcy52ZXJ0ZXhJZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLnZlcnRleCA9IGRhdGE7XG4gICAgfSk7XG4gIH07XG4gIGdldFZlcnRleCgpO1xuICByZXR1cm4gJHNjb3BlLiRvbigncmVsb2FkJywgZnVuY3Rpb24oZXZlbnQpIHtcbiAgICByZXR1cm4gZ2V0VmVydGV4KCk7XG4gIH0pO1xufSkuY29udHJvbGxlcignSm9iRXhjZXB0aW9uc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZSwgJHN0YXRlUGFyYW1zLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gSm9ic1NlcnZpY2UubG9hZEV4Y2VwdGlvbnMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLmV4Y2VwdGlvbnMgPSBkYXRhO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ0pvYlByb3BlcnRpZXNDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSkge1xuICByZXR1cm4gJHNjb3BlLmNoYW5nZU5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICBpZiAobm9kZWlkICE9PSAkc2NvcGUubm9kZWlkKSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbm9kZWlkO1xuICAgICAgcmV0dXJuIEpvYnNTZXJ2aWNlLmdldE5vZGUobm9kZWlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRzY29wZS5ub2RlID0gZGF0YTtcbiAgICAgIH0pO1xuICAgIH0gZWxzZSB7XG4gICAgICAkc2NvcGUubm9kZWlkID0gbnVsbDtcbiAgICAgIHJldHVybiAkc2NvcGUubm9kZSA9IG51bGw7XG4gICAgfVxuICB9O1xufSkuY29udHJvbGxlcignSm9iUGxhbk1ldHJpY3NDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCBKb2JzU2VydmljZSwgTWV0cmljc1NlcnZpY2UpIHtcbiAgdmFyIGFscGhhYmV0aWNhbFNvcnRCeUlkLCBsb2FkTWV0cmljcztcbiAgJHNjb3BlLmRyYWdnaW5nID0gZmFsc2U7XG4gICRzY29wZS53aW5kb3cgPSBNZXRyaWNzU2VydmljZS5nZXRXaW5kb3coKTtcbiAgJHNjb3BlLmF2YWlsYWJsZU1ldHJpY3MgPSBudWxsO1xuICAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBNZXRyaWNzU2VydmljZS51blJlZ2lzdGVyT2JzZXJ2ZXIoKTtcbiAgfSk7XG4gIGxvYWRNZXRyaWNzID0gZnVuY3Rpb24oKSB7XG4gICAgSm9ic1NlcnZpY2UuZ2V0VmVydGV4KCRzY29wZS5ub2RlaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS52ZXJ0ZXggPSBkYXRhO1xuICAgIH0pO1xuICAgIHJldHVybiBNZXRyaWNzU2VydmljZS5nZXRBdmFpbGFibGVNZXRyaWNzKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYXZhaWxhYmxlTWV0cmljcyA9IGRhdGEuc29ydChhbHBoYWJldGljYWxTb3J0QnlJZCk7XG4gICAgICAkc2NvcGUubWV0cmljcyA9IE1ldHJpY3NTZXJ2aWNlLmdldE1ldHJpY3NTZXR1cCgkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQpLm5hbWVzO1xuICAgICAgcmV0dXJuIE1ldHJpY3NTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYmlkLCAkc2NvcGUubm9kZWlkLCBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkc2NvcGUuJGJyb2FkY2FzdChcIm1ldHJpY3M6ZGF0YTp1cGRhdGVcIiwgZGF0YS50aW1lc3RhbXAsIGRhdGEudmFsdWVzKTtcbiAgICAgIH0pO1xuICAgIH0pO1xuICB9O1xuICBhbHBoYWJldGljYWxTb3J0QnlJZCA9IGZ1bmN0aW9uKGEsIGIpIHtcbiAgICB2YXIgQSwgQjtcbiAgICBBID0gYS5pZC50b0xvd2VyQ2FzZSgpO1xuICAgIEIgPSBiLmlkLnRvTG93ZXJDYXNlKCk7XG4gICAgaWYgKEEgPCBCKSB7XG4gICAgICByZXR1cm4gLTE7XG4gICAgfSBlbHNlIGlmIChBID4gQikge1xuICAgICAgcmV0dXJuIDE7XG4gICAgfSBlbHNlIHtcbiAgICAgIHJldHVybiAwO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmRyb3BwZWQgPSBmdW5jdGlvbihldmVudCwgaW5kZXgsIGl0ZW0sIGV4dGVybmFsLCB0eXBlKSB7XG4gICAgTWV0cmljc1NlcnZpY2Uub3JkZXJNZXRyaWNzKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgaXRlbSwgaW5kZXgpO1xuICAgICRzY29wZS4kYnJvYWRjYXN0KFwibWV0cmljczpyZWZyZXNoXCIsIGl0ZW0pO1xuICAgIGxvYWRNZXRyaWNzKCk7XG4gICAgcmV0dXJuIGZhbHNlO1xuICB9O1xuICAkc2NvcGUuZHJhZ1N0YXJ0ID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5kcmFnZ2luZyA9IHRydWU7XG4gIH07XG4gICRzY29wZS5kcmFnRW5kID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRzY29wZS5kcmFnZ2luZyA9IGZhbHNlO1xuICB9O1xuICAkc2NvcGUuYWRkTWV0cmljID0gZnVuY3Rpb24obWV0cmljKSB7XG4gICAgTWV0cmljc1NlcnZpY2UuYWRkTWV0cmljKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgbWV0cmljLmlkKTtcbiAgICByZXR1cm4gbG9hZE1ldHJpY3MoKTtcbiAgfTtcbiAgJHNjb3BlLnJlbW92ZU1ldHJpYyA9IGZ1bmN0aW9uKG1ldHJpYykge1xuICAgIE1ldHJpY3NTZXJ2aWNlLnJlbW92ZU1ldHJpYygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYyk7XG4gICAgcmV0dXJuIGxvYWRNZXRyaWNzKCk7XG4gIH07XG4gICRzY29wZS5zZXRNZXRyaWNTaXplID0gZnVuY3Rpb24obWV0cmljLCBzaXplKSB7XG4gICAgTWV0cmljc1NlcnZpY2Uuc2V0TWV0cmljU2l6ZSgkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYywgc2l6ZSk7XG4gICAgcmV0dXJuIGxvYWRNZXRyaWNzKCk7XG4gIH07XG4gICRzY29wZS5zZXRNZXRyaWNWaWV3ID0gZnVuY3Rpb24obWV0cmljLCB2aWV3KSB7XG4gICAgTWV0cmljc1NlcnZpY2Uuc2V0TWV0cmljVmlldygkc2NvcGUuam9iaWQsICRzY29wZS5ub2RlaWQsIG1ldHJpYywgdmlldyk7XG4gICAgcmV0dXJuIGxvYWRNZXRyaWNzKCk7XG4gIH07XG4gICRzY29wZS5nZXRWYWx1ZXMgPSBmdW5jdGlvbihtZXRyaWMpIHtcbiAgICByZXR1cm4gTWV0cmljc1NlcnZpY2UuZ2V0VmFsdWVzKCRzY29wZS5qb2JpZCwgJHNjb3BlLm5vZGVpZCwgbWV0cmljKTtcbiAgfTtcbiAgJHNjb3BlLiRvbignbm9kZTpjaGFuZ2UnLCBmdW5jdGlvbihldmVudCwgbm9kZWlkKSB7XG4gICAgaWYgKCEkc2NvcGUuZHJhZ2dpbmcpIHtcbiAgICAgIHJldHVybiBsb2FkTWV0cmljcygpO1xuICAgIH1cbiAgfSk7XG4gIGlmICgkc2NvcGUubm9kZWlkKSB7XG4gICAgcmV0dXJuIGxvYWRNZXRyaWNzKCk7XG4gIH1cbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ3ZlcnRleCcsICgkc3RhdGUpIC0+XHJcbiAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUgc2Vjb25kYXJ5JyB3aWR0aD0nMCcgaGVpZ2h0PScwJz48L3N2Zz5cIlxyXG5cclxuICBzY29wZTpcclxuICAgIGRhdGE6IFwiPVwiXHJcblxyXG4gIGxpbms6IChzY29wZSwgZWxlbSwgYXR0cnMpIC0+XHJcbiAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXVxyXG5cclxuICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKClcclxuICAgIGFuZ3VsYXIuZWxlbWVudChzdmdFbCkuYXR0cignd2lkdGgnLCBjb250YWluZXJXKVxyXG5cclxuICAgIGFuYWx5emVUaW1lID0gKGRhdGEpIC0+XHJcbiAgICAgIGQzLnNlbGVjdChzdmdFbCkuc2VsZWN0QWxsKFwiKlwiKS5yZW1vdmUoKVxyXG5cclxuICAgICAgdGVzdERhdGEgPSBbXVxyXG5cclxuICAgICAgYW5ndWxhci5mb3JFYWNoIGRhdGEuc3VidGFza3MsIChzdWJ0YXNrLCBpKSAtPlxyXG4gICAgICAgIHRpbWVzID0gW1xyXG4gICAgICAgICAge1xyXG4gICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIlxyXG4gICAgICAgICAgICBjb2xvcjogXCIjNjY2XCJcclxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXHJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlNDSEVEVUxFRFwiXVxyXG4gICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdXHJcbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xyXG4gICAgICAgICAgfVxyXG4gICAgICAgICAge1xyXG4gICAgICAgICAgICBsYWJlbDogXCJEZXBsb3lpbmdcIlxyXG4gICAgICAgICAgICBjb2xvcjogXCIjYWFhXCJcclxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXHJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXVxyXG4gICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXVxyXG4gICAgICAgICAgICB0eXBlOiAncmVndWxhcidcclxuICAgICAgICAgIH1cclxuICAgICAgICBdXHJcblxyXG4gICAgICAgIGlmIHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdID4gMFxyXG4gICAgICAgICAgdGltZXMucHVzaCB7XHJcbiAgICAgICAgICAgIGxhYmVsOiBcIlJ1bm5pbmdcIlxyXG4gICAgICAgICAgICBjb2xvcjogXCIjZGRkXCJcclxuICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NVwiXHJcbiAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlJVTk5JTkdcIl1cclxuICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdXHJcbiAgICAgICAgICAgIHR5cGU6ICdyZWd1bGFyJ1xyXG4gICAgICAgICAgfVxyXG5cclxuICAgICAgICB0ZXN0RGF0YS5wdXNoIHtcclxuICAgICAgICAgIGxhYmVsOiBcIigje3N1YnRhc2suc3VidGFza30pICN7c3VidGFzay5ob3N0fVwiXHJcbiAgICAgICAgICB0aW1lczogdGltZXNcclxuICAgICAgICB9XHJcblxyXG4gICAgICBjaGFydCA9IGQzLnRpbWVsaW5lKCkuc3RhY2soKVxyXG4gICAgICAudGlja0Zvcm1hdCh7XHJcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXHJcbiAgICAgICAgIyB0aWNrSW50ZXJ2YWw6IDFcclxuICAgICAgICB0aWNrU2l6ZTogMVxyXG4gICAgICB9KVxyXG4gICAgICAucHJlZml4KFwic2luZ2xlXCIpXHJcbiAgICAgIC5sYWJlbEZvcm1hdCgobGFiZWwpIC0+XHJcbiAgICAgICAgbGFiZWxcclxuICAgICAgKVxyXG4gICAgICAubWFyZ2luKHsgbGVmdDogMTAwLCByaWdodDogMCwgdG9wOiAwLCBib3R0b206IDAgfSlcclxuICAgICAgLml0ZW1IZWlnaHQoMzApXHJcbiAgICAgIC5yZWxhdGl2ZVRpbWUoKVxyXG5cclxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxyXG4gICAgICAuZGF0dW0odGVzdERhdGEpXHJcbiAgICAgIC5jYWxsKGNoYXJ0KVxyXG5cclxuICAgIGFuYWx5emVUaW1lKHNjb3BlLmRhdGEpXHJcblxyXG4gICAgcmV0dXJuXHJcblxyXG4jIC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS1cclxuXHJcbi5kaXJlY3RpdmUgJ3RpbWVsaW5lJywgKCRzdGF0ZSkgLT5cclxuICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSd0aW1lbGluZScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCJcclxuXHJcbiAgc2NvcGU6XHJcbiAgICB2ZXJ0aWNlczogXCI9XCJcclxuICAgIGpvYmlkOiBcIj1cIlxyXG5cclxuICBsaW5rOiAoc2NvcGUsIGVsZW0sIGF0dHJzKSAtPlxyXG4gICAgc3ZnRWwgPSBlbGVtLmNoaWxkcmVuKClbMF1cclxuXHJcbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXHJcbiAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVylcclxuXHJcbiAgICB0cmFuc2xhdGVMYWJlbCA9IChsYWJlbCkgLT5cclxuICAgICAgbGFiZWwucmVwbGFjZShcIiZndDtcIiwgXCI+XCIpXHJcblxyXG4gICAgYW5hbHl6ZVRpbWUgPSAoZGF0YSkgLT5cclxuICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXHJcblxyXG4gICAgICB0ZXN0RGF0YSA9IFtdXHJcblxyXG4gICAgICBhbmd1bGFyLmZvckVhY2ggZGF0YSwgKHZlcnRleCkgLT5cclxuICAgICAgICBpZiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSA+IC0xXHJcbiAgICAgICAgICBpZiB2ZXJ0ZXgudHlwZSBpcyAnc2NoZWR1bGVkJ1xyXG4gICAgICAgICAgICB0ZXN0RGF0YS5wdXNoXHJcbiAgICAgICAgICAgICAgdGltZXM6IFtcclxuICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSlcclxuICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNjY2NjY2NcIlxyXG4gICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NTU1NVwiXHJcbiAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXVxyXG4gICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXVxyXG4gICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcclxuICAgICAgICAgICAgICBdXHJcbiAgICAgICAgICBlbHNlXHJcbiAgICAgICAgICAgIHRlc3REYXRhLnB1c2hcclxuICAgICAgICAgICAgICB0aW1lczogW1xyXG4gICAgICAgICAgICAgICAgbGFiZWw6IHRyYW5zbGF0ZUxhYmVsKHZlcnRleC5uYW1lKVxyXG4gICAgICAgICAgICAgICAgY29sb3I6IFwiI2Q5ZjFmN1wiXHJcbiAgICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNjJjZGVhXCJcclxuICAgICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHZlcnRleFsnc3RhcnQtdGltZSddXHJcbiAgICAgICAgICAgICAgICBlbmRpbmdfdGltZTogdmVydGV4WydlbmQtdGltZSddXHJcbiAgICAgICAgICAgICAgICBsaW5rOiB2ZXJ0ZXguaWRcclxuICAgICAgICAgICAgICAgIHR5cGU6IHZlcnRleC50eXBlXHJcbiAgICAgICAgICAgICAgXVxyXG5cclxuICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soKGQsIGksIGRhdHVtKSAtPlxyXG4gICAgICAgIGlmIGQubGlua1xyXG4gICAgICAgICAgJHN0YXRlLmdvIFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwgeyBqb2JpZDogc2NvcGUuam9iaWQsIHZlcnRleElkOiBkLmxpbmsgfVxyXG5cclxuICAgICAgKVxyXG4gICAgICAudGlja0Zvcm1hdCh7XHJcbiAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpXHJcbiAgICAgICAgIyB0aWNrVGltZTogZDMudGltZS5zZWNvbmRcclxuICAgICAgICAjIHRpY2tJbnRlcnZhbDogMC41XHJcbiAgICAgICAgdGlja1NpemU6IDFcclxuICAgICAgfSlcclxuICAgICAgLnByZWZpeChcIm1haW5cIilcclxuICAgICAgLm1hcmdpbih7IGxlZnQ6IDAsIHJpZ2h0OiAwLCB0b3A6IDAsIGJvdHRvbTogMCB9KVxyXG4gICAgICAuaXRlbUhlaWdodCgzMClcclxuICAgICAgLnNob3dCb3JkZXJMaW5lKClcclxuICAgICAgLnNob3dIb3VyVGltZWxpbmUoKVxyXG5cclxuICAgICAgc3ZnID0gZDMuc2VsZWN0KHN2Z0VsKVxyXG4gICAgICAuZGF0dW0odGVzdERhdGEpXHJcbiAgICAgIC5jYWxsKGNoYXJ0KVxyXG5cclxuICAgIHNjb3BlLiR3YXRjaCBhdHRycy52ZXJ0aWNlcywgKGRhdGEpIC0+XHJcbiAgICAgIGFuYWx5emVUaW1lKGRhdGEpIGlmIGRhdGFcclxuXHJcbiAgICByZXR1cm5cclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAnc3BsaXQnLCAoKSAtPlxyXG4gIHJldHVybiBjb21waWxlOiAodEVsZW0sIHRBdHRycykgLT5cclxuICAgICAgU3BsaXQodEVsZW0uY2hpbGRyZW4oKSwgKFxyXG4gICAgICAgIHNpemVzOiBbNTAsIDUwXVxyXG4gICAgICAgIGRpcmVjdGlvbjogJ3ZlcnRpY2FsJ1xyXG4gICAgICApKVxyXG5cclxuIyAtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tXHJcblxyXG4uZGlyZWN0aXZlICdqb2JQbGFuJywgKCR0aW1lb3V0KSAtPlxyXG4gIHRlbXBsYXRlOiBcIlxyXG4gICAgPHN2ZyBjbGFzcz0nZ3JhcGgnPjxnIC8+PC9zdmc+XHJcbiAgICA8c3ZnIGNsYXNzPSd0bXAnIHdpZHRoPScxJyBoZWlnaHQ9JzEnPjxnIC8+PC9zdmc+XHJcbiAgICA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz5cclxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT5cclxuICAgICAgPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT5cclxuICAgIDwvZGl2PlwiXHJcblxyXG4gIHNjb3BlOlxyXG4gICAgcGxhbjogJz0nXHJcbiAgICB3YXRlcm1hcmtzOiAnPSdcclxuICAgIHNldE5vZGU6ICcmJ1xyXG5cclxuICBsaW5rOiAoc2NvcGUsIGVsZW0sIGF0dHJzKSAtPlxyXG4gICAgZyA9IG51bGxcclxuICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpXHJcbiAgICBzdWJncmFwaHMgPSBbXVxyXG4gICAgam9iaWQgPSBhdHRycy5qb2JpZFxyXG5cclxuICAgIG1haW5TdmdFbGVtZW50ID0gZWxlbS5jaGlsZHJlbigpWzBdXHJcbiAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdXHJcbiAgICBtYWluVG1wRWxlbWVudCA9IGVsZW0uY2hpbGRyZW4oKVsxXVxyXG5cclxuICAgIGQzbWFpblN2ZyA9IGQzLnNlbGVjdChtYWluU3ZnRWxlbWVudClcclxuICAgIGQzbWFpblN2Z0cgPSBkMy5zZWxlY3QobWFpbkcpXHJcbiAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudClcclxuXHJcbiAgICAjIGFuZ3VsYXIuZWxlbWVudChtYWluRykuZW1wdHkoKVxyXG4gICAgIyBkM21haW5TdmdHLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKClcclxuXHJcbiAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpXHJcbiAgICBhbmd1bGFyLmVsZW1lbnQoZWxlbS5jaGlsZHJlbigpWzBdKS53aWR0aChjb250YWluZXJXKVxyXG5cclxuICAgIGxhc3Rab29tU2NhbGUgPSAwXHJcbiAgICBsYXN0UG9zaXRpb24gPSAwXHJcblxyXG4gICAgc2NvcGUuem9vbUluID0gLT5cclxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA8IDIuOTlcclxuXHJcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gem9vbSBvYmplY3RcclxuICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKVxyXG4gICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgKyAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpXHJcbiAgICAgICAgdjIgPSB0cmFuc2xhdGVbMV0gKiAobWFpblpvb20uc2NhbGUoKSArIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcclxuICAgICAgICBtYWluWm9vbS5zY2FsZSBtYWluWm9vbS5zY2FsZSgpICsgMC4xXHJcbiAgICAgICAgbWFpblpvb20udHJhbnNsYXRlIFsgdjEsIHYyIF1cclxuXHJcbiAgICAgICAgIyBUcmFuc2Zvcm0gc3ZnXHJcbiAgICAgICAgZDNtYWluU3ZnRy5hdHRyIFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiXHJcblxyXG4gICAgICAgIGxhc3Rab29tU2NhbGUgPSBtYWluWm9vbS5zY2FsZSgpXHJcbiAgICAgICAgbGFzdFBvc2l0aW9uID0gbWFpblpvb20udHJhbnNsYXRlKClcclxuXHJcbiAgICBzY29wZS56b29tT3V0ID0gLT5cclxuICAgICAgaWYgbWFpblpvb20uc2NhbGUoKSA+IDAuMzFcclxuXHJcbiAgICAgICAgIyBDYWxjdWxhdGUgYW5kIHN0b3JlIG5ldyB2YWx1ZXMgaW4gbWFpblpvb20gb2JqZWN0XHJcbiAgICAgICAgbWFpblpvb20uc2NhbGUgbWFpblpvb20uc2NhbGUoKSAtIDAuMVxyXG4gICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXHJcbiAgICAgICAgdjEgPSB0cmFuc2xhdGVbMF0gKiAobWFpblpvb20uc2NhbGUoKSAtIDAuMSAvIChtYWluWm9vbS5zY2FsZSgpKSlcclxuICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpIC0gMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKVxyXG4gICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZSBbIHYxLCB2MiBdXHJcblxyXG4gICAgICAgICMgVHJhbnNmb3JtIHN2Z1xyXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0ciBcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIlxyXG5cclxuICAgICAgICBsYXN0Wm9vbVNjYWxlID0gbWFpblpvb20uc2NhbGUoKVxyXG4gICAgICAgIGxhc3RQb3NpdGlvbiA9IG1haW5ab29tLnRyYW5zbGF0ZSgpXHJcblxyXG4gICAgI2NyZWF0ZSBhIGxhYmVsIG9mIGFuIGVkZ2VcclxuICAgIGNyZWF0ZUxhYmVsRWRnZSA9IChlbCkgLT5cclxuICAgICAgbGFiZWxWYWx1ZSA9IFwiXCJcclxuICAgICAgaWYgZWwuc2hpcF9zdHJhdGVneT8gb3IgZWwubG9jYWxfc3RyYXRlZ3k/XHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxkaXYgY2xhc3M9J2VkZ2UtbGFiZWwnPlwiXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5ICBpZiBlbC5zaGlwX3N0cmF0ZWd5P1xyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCIgIHVubGVzcyBlbC50ZW1wX21vZGUgaXMgYHVuZGVmaW5lZGBcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5ICB1bmxlc3MgZWwubG9jYWxfc3RyYXRlZ3kgaXMgYHVuZGVmaW5lZGBcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCJcclxuICAgICAgbGFiZWxWYWx1ZVxyXG5cclxuXHJcbiAgICAjIHRydWUsIGlmIHRoZSBub2RlIGlzIGEgc3BlY2lhbCBub2RlIGZyb20gYW4gaXRlcmF0aW9uXHJcbiAgICBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlID0gKGluZm8pIC0+XHJcbiAgICAgIChpbmZvIGlzIFwicGFydGlhbFNvbHV0aW9uXCIgb3IgaW5mbyBpcyBcIm5leHRQYXJ0aWFsU29sdXRpb25cIiBvciBpbmZvIGlzIFwid29ya3NldFwiIG9yIGluZm8gaXMgXCJuZXh0V29ya3NldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvblNldFwiIG9yIGluZm8gaXMgXCJzb2x1dGlvbkRlbHRhXCIpXHJcblxyXG4gICAgZ2V0Tm9kZVR5cGUgPSAoZWwsIGluZm8pIC0+XHJcbiAgICAgIGlmIGluZm8gaXMgXCJtaXJyb3JcIlxyXG4gICAgICAgICdub2RlLW1pcnJvcidcclxuXHJcbiAgICAgIGVsc2UgaWYgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKVxyXG4gICAgICAgICdub2RlLWl0ZXJhdGlvbidcclxuXHJcbiAgICAgIGVsc2VcclxuICAgICAgICAnbm9kZS1ub3JtYWwnXHJcblxyXG4gICAgIyBjcmVhdGVzIHRoZSBsYWJlbCBvZiBhIG5vZGUsIGluIGluZm8gaXMgc3RvcmVkLCB3aGV0aGVyIGl0IGlzIGEgc3BlY2lhbCBub2RlIChsaWtlIGEgbWlycm9yIGluIGFuIGl0ZXJhdGlvbilcclxuICAgIGNyZWF0ZUxhYmVsTm9kZSA9IChlbCwgaW5mbywgbWF4VywgbWF4SCkgLT5cclxuICAgICAgIyBsYWJlbFZhbHVlID0gXCI8YSBocmVmPScjL2pvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRleC9cIiArIGVsLmlkICsgXCInIGNsYXNzPSdub2RlLWxhYmVsIFwiICsgZ2V0Tm9kZVR5cGUoZWwsIGluZm8pICsgXCInPlwiXHJcbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxkaXYgaHJlZj0nIy9qb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0ZXgvXCIgKyBlbC5pZCArIFwiJyBjbGFzcz0nbm9kZS1sYWJlbCBcIiArIGdldE5vZGVUeXBlKGVsLCBpbmZvKSArIFwiJz5cIlxyXG5cclxuICAgICAgIyBOb2RlbmFtZVxyXG4gICAgICBpZiBpbmZvIGlzIFwibWlycm9yXCJcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPk1pcnJvciBvZiBcIiArIGVsLm9wZXJhdG9yICsgXCI8L2gzPlwiXHJcbiAgICAgIGVsc2VcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCJcclxuICAgICAgaWYgZWwuZGVzY3JpcHRpb24gaXMgXCJcIlxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCJcIlxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgc3RlcE5hbWUgPSBlbC5kZXNjcmlwdGlvblxyXG5cclxuICAgICAgICAjIGNsZWFuIHN0ZXBOYW1lXHJcbiAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKVxyXG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDQgY2xhc3M9J3N0ZXAtbmFtZSc+XCIgKyBzdGVwTmFtZSArIFwiPC9oND5cIlxyXG5cclxuICAgICAgIyBJZiB0aGlzIG5vZGUgaXMgYW4gXCJpdGVyYXRpb25cIiB3ZSBuZWVkIGEgZGlmZmVyZW50IHBhbmVsLWJvZHlcclxuICAgICAgaWYgZWwuc3RlcF9mdW5jdGlvbj9cclxuICAgICAgICBsYWJlbFZhbHVlICs9IGV4dGVuZExhYmVsTm9kZUZvckl0ZXJhdGlvbihlbC5pZCwgbWF4VywgbWF4SClcclxuICAgICAgZWxzZVxyXG5cclxuICAgICAgICAjIE90aGVyd2lzZSBhZGQgaW5mb3NcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlwiICsgaW5mbyArIFwiIE5vZGU8L2g1PlwiICBpZiBpc1NwZWNpYWxJdGVyYXRpb25Ob2RlKGluZm8pXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5QYXJhbGxlbGlzbTogXCIgKyBlbC5wYXJhbGxlbGlzbSArIFwiPC9oNT5cIiAgdW5sZXNzIGVsLnBhcmFsbGVsaXNtIGlzIFwiXCJcclxuICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PkxvdyBXYXRlcm1hcms6IFwiICsgZWwubG93V2F0ZXJtYXJrICsgXCI8L2g1PlwiICB1bmxlc3MgZWwubG93V2F0ZXJtYXJrIGlzIGB1bmRlZmluZWRgXHJcbiAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCIgdW5sZXNzIGVsLm9wZXJhdG9yIGlzIGB1bmRlZmluZWRgIG9yIG5vdCBlbC5vcGVyYXRvcl9zdHJhdGVneVxyXG4gICAgICAjIGxhYmVsVmFsdWUgKz0gXCI8L2E+XCJcclxuICAgICAgbGFiZWxWYWx1ZSArPSBcIjwvZGl2PlwiXHJcbiAgICAgIGxhYmVsVmFsdWVcclxuXHJcbiAgICAjIEV4dGVuZHMgdGhlIGxhYmVsIG9mIGEgbm9kZSB3aXRoIGFuIGFkZGl0aW9uYWwgc3ZnIEVsZW1lbnQgdG8gcHJlc2VudCB0aGUgaXRlcmF0aW9uLlxyXG4gICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gKGlkLCBtYXhXLCBtYXhIKSAtPlxyXG4gICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWRcclxuXHJcbiAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCJcclxuICAgICAgbGFiZWxWYWx1ZVxyXG5cclxuICAgICMgU3BsaXQgYSBzdHJpbmcgaW50byBtdWx0aXBsZSBsaW5lcyBzbyB0aGF0IGVhY2ggbGluZSBoYXMgbGVzcyB0aGFuIDMwIGxldHRlcnMuXHJcbiAgICBzaG9ydGVuU3RyaW5nID0gKHMpIC0+XHJcbiAgICAgICMgbWFrZSBzdXJlIHRoYXQgbmFtZSBkb2VzIG5vdCBjb250YWluIGEgPCAoYmVjYXVzZSBvZiBodG1sKVxyXG4gICAgICBpZiBzLmNoYXJBdCgwKSBpcyBcIjxcIlxyXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI8XCIsIFwiJmx0O1wiKVxyXG4gICAgICAgIHMgPSBzLnJlcGxhY2UoXCI+XCIsIFwiJmd0O1wiKVxyXG4gICAgICBzYnIgPSBcIlwiXHJcbiAgICAgIHdoaWxlIHMubGVuZ3RoID4gMzBcclxuICAgICAgICBzYnIgPSBzYnIgKyBzLnN1YnN0cmluZygwLCAzMCkgKyBcIjxicj5cIlxyXG4gICAgICAgIHMgPSBzLnN1YnN0cmluZygzMCwgcy5sZW5ndGgpXHJcbiAgICAgIHNiciA9IHNiciArIHNcclxuICAgICAgc2JyXHJcblxyXG4gICAgY3JlYXRlTm9kZSA9IChnLCBkYXRhLCBlbCwgaXNQYXJlbnQgPSBmYWxzZSwgbWF4VywgbWF4SCkgLT5cclxuICAgICAgIyBjcmVhdGUgbm9kZSwgc2VuZCBhZGRpdGlvbmFsIGluZm9ybWF0aW9ucyBhYm91dCB0aGUgbm9kZSBpZiBpdCBpcyBhIHNwZWNpYWwgb25lXHJcbiAgICAgIGlmIGVsLmlkIGlzIGRhdGEucGFydGlhbF9zb2x1dGlvblxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpXHJcbiAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgICAgY2xhc3M6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxyXG5cclxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvblxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXHJcblxyXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEud29ya3NldFxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwid29ya3NldFwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXHJcblxyXG4gICAgICBlbHNlIGlmIGVsLmlkIGlzIGRhdGEubmV4dF93b3Jrc2V0XHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0V29ya3NldFwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxyXG5cclxuICAgICAgZWxzZSBpZiBlbC5pZCBpcyBkYXRhLnNvbHV0aW9uX3NldFxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25TZXRcIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcclxuXHJcbiAgICAgIGVsc2UgaWYgZWwuaWQgaXMgZGF0YS5zb2x1dGlvbl9kZWx0YVxyXG4gICAgICAgIGcuc2V0Tm9kZSBlbC5pZCxcclxuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25EZWx0YVwiLCBtYXhXLCBtYXhIKVxyXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCdcclxuICAgICAgICAgIGNsYXNzOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXHJcblxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgZy5zZXROb2RlIGVsLmlkLFxyXG4gICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJcIiwgbWF4VywgbWF4SClcclxuICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnXHJcbiAgICAgICAgICBjbGFzczogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXHJcblxyXG4gICAgY3JlYXRlRWRnZSA9IChnLCBkYXRhLCBlbCwgZXhpc3RpbmdOb2RlcywgcHJlZCkgLT5cclxuICAgICAgZy5zZXRFZGdlIHByZWQuaWQsIGVsLmlkLFxyXG4gICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZClcclxuICAgICAgICBsYWJlbFR5cGU6ICdodG1sJ1xyXG4gICAgICAgIGFycm93aGVhZDogJ25vcm1hbCdcclxuXHJcbiAgICBsb2FkSnNvblRvRGFncmUgPSAoZywgZGF0YSkgLT5cclxuICAgICAgZXhpc3RpbmdOb2RlcyA9IFtdXHJcblxyXG4gICAgICBpZiBkYXRhLm5vZGVzP1xyXG4gICAgICAgICMgVGhpcyBpcyB0aGUgbm9ybWFsIGpzb24gZGF0YVxyXG4gICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEubm9kZXNcclxuXHJcbiAgICAgIGVsc2VcclxuICAgICAgICAjIFRoaXMgaXMgYW4gaXRlcmF0aW9uLCB3ZSBub3cgc3RvcmUgc3BlY2lhbCBpdGVyYXRpb24gbm9kZXMgaWYgcG9zc2libGVcclxuICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLnN0ZXBfZnVuY3Rpb25cclxuICAgICAgICBpc1BhcmVudCA9IHRydWVcclxuXHJcbiAgICAgIGZvciBlbCBpbiB0b0l0ZXJhdGVcclxuICAgICAgICBtYXhXID0gMFxyXG4gICAgICAgIG1heEggPSAwXHJcblxyXG4gICAgICAgIGlmIGVsLnN0ZXBfZnVuY3Rpb25cclxuICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoeyBtdWx0aWdyYXBoOiB0cnVlLCBjb21wb3VuZDogdHJ1ZSB9KS5zZXRHcmFwaCh7XHJcbiAgICAgICAgICAgIG5vZGVzZXA6IDIwXHJcbiAgICAgICAgICAgIGVkZ2VzZXA6IDBcclxuICAgICAgICAgICAgcmFua3NlcDogMjBcclxuICAgICAgICAgICAgcmFua2RpcjogXCJMUlwiXHJcbiAgICAgICAgICAgIG1hcmdpbng6IDEwXHJcbiAgICAgICAgICAgIG1hcmdpbnk6IDEwXHJcbiAgICAgICAgICAgIH0pXHJcblxyXG4gICAgICAgICAgc3ViZ3JhcGhzW2VsLmlkXSA9IHNnXHJcblxyXG4gICAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKHNnLCBlbClcclxuXHJcbiAgICAgICAgICByID0gbmV3IGRhZ3JlRDMucmVuZGVyKClcclxuICAgICAgICAgIGQzdG1wU3ZnLnNlbGVjdCgnZycpLmNhbGwociwgc2cpXHJcbiAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aFxyXG4gICAgICAgICAgbWF4SCA9IHNnLmdyYXBoKCkuaGVpZ2h0XHJcblxyXG4gICAgICAgICAgYW5ndWxhci5lbGVtZW50KG1haW5UbXBFbGVtZW50KS5lbXB0eSgpXHJcblxyXG4gICAgICAgIGNyZWF0ZU5vZGUoZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKVxyXG5cclxuICAgICAgICBleGlzdGluZ05vZGVzLnB1c2ggZWwuaWRcclxuXHJcbiAgICAgICAgIyBjcmVhdGUgZWRnZXMgZnJvbSBpbnB1dHMgdG8gY3VycmVudCBub2RlXHJcbiAgICAgICAgaWYgZWwuaW5wdXRzP1xyXG4gICAgICAgICAgZm9yIHByZWQgaW4gZWwuaW5wdXRzXHJcbiAgICAgICAgICAgIGNyZWF0ZUVkZ2UoZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpXHJcblxyXG4gICAgICBnXHJcblxyXG4gICAgIyBzZWFyY2hlcyBpbiB0aGUgZ2xvYmFsIEpTT05EYXRhIGZvciB0aGUgbm9kZSB3aXRoIHRoZSBnaXZlbiBpZFxyXG4gICAgc2VhcmNoRm9yTm9kZSA9IChkYXRhLCBub2RlSUQpIC0+XHJcbiAgICAgIGZvciBpIG9mIGRhdGEubm9kZXNcclxuICAgICAgICBlbCA9IGRhdGEubm9kZXNbaV1cclxuICAgICAgICByZXR1cm4gZWwgIGlmIGVsLmlkIGlzIG5vZGVJRFxyXG5cclxuICAgICAgICAjIGxvb2sgZm9yIG5vZGVzIHRoYXQgYXJlIGluIGl0ZXJhdGlvbnNcclxuICAgICAgICBpZiBlbC5zdGVwX2Z1bmN0aW9uP1xyXG4gICAgICAgICAgZm9yIGogb2YgZWwuc3RlcF9mdW5jdGlvblxyXG4gICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXSAgaWYgZWwuc3RlcF9mdW5jdGlvbltqXS5pZCBpcyBub2RlSURcclxuXHJcbiAgICBtZXJnZVdhdGVybWFya3MgPSAoZGF0YSwgd2F0ZXJtYXJrcykgLT5cclxuICAgICAgaWYgKCFfLmlzRW1wdHkod2F0ZXJtYXJrcykpXHJcbiAgICAgICAgZm9yIG5vZGUgaW4gZGF0YS5ub2Rlc1xyXG4gICAgICAgICAgaWYgKHdhdGVybWFya3Nbbm9kZS5pZF0gJiYgIWlzTmFOKHdhdGVybWFya3Nbbm9kZS5pZF1bXCJsb3dXYXRlcm1hcmtcIl0pKVxyXG4gICAgICAgICAgICBub2RlLmxvd1dhdGVybWFyayA9IHdhdGVybWFya3Nbbm9kZS5pZF1bXCJsb3dXYXRlcm1hcmtcIl1cclxuXHJcbiAgICAgIHJldHVybiBkYXRhXHJcblxyXG4gICAgbGFzdFBvc2l0aW9uID0gMFxyXG4gICAgbGFzdFpvb21TY2FsZSA9IDBcclxuXHJcbiAgICBkcmF3R3JhcGggPSAoKSAtPlxyXG4gICAgICBpZiBzY29wZS5wbGFuXHJcbiAgICAgICAgZyA9IG5ldyBkYWdyZUQzLmdyYXBobGliLkdyYXBoKHsgbXVsdGlncmFwaDogdHJ1ZSwgY29tcG91bmQ6IHRydWUgfSkuc2V0R3JhcGgoe1xyXG4gICAgICAgICAgbm9kZXNlcDogNzBcclxuICAgICAgICAgIGVkZ2VzZXA6IDBcclxuICAgICAgICAgIHJhbmtzZXA6IDUwXHJcbiAgICAgICAgICByYW5rZGlyOiBcIkxSXCJcclxuICAgICAgICAgIG1hcmdpbng6IDQwXHJcbiAgICAgICAgICBtYXJnaW55OiA0MFxyXG4gICAgICAgICAgfSlcclxuXHJcbiAgICAgICAgbG9hZEpzb25Ub0RhZ3JlKGcsIG1lcmdlV2F0ZXJtYXJrcyhzY29wZS5wbGFuLCBzY29wZS53YXRlcm1hcmtzKSlcclxuXHJcbiAgICAgICAgZDNtYWluU3ZnRy5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpXHJcblxyXG4gICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInNjYWxlKFwiICsgMSArIFwiKVwiKVxyXG5cclxuICAgICAgICByZW5kZXJlciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpXHJcbiAgICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKVxyXG5cclxuICAgICAgICBmb3IgaSwgc2cgb2Ygc3ViZ3JhcGhzXHJcbiAgICAgICAgICBkM21haW5Tdmcuc2VsZWN0KCdzdmcuc3ZnLScgKyBpICsgJyBnJykuY2FsbChyZW5kZXJlciwgc2cpXHJcblxyXG4gICAgICAgIG5ld1NjYWxlID0gMC41XHJcblxyXG4gICAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKVxyXG4gICAgICAgIHlDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLmhlaWdodCgpIC0gZy5ncmFwaCgpLmhlaWdodCAqIG5ld1NjYWxlKSAvIDIpXHJcblxyXG4gICAgICAgIGlmIGxhc3Rab29tU2NhbGUgIT0gMCAmJiBsYXN0UG9zaXRpb24gIT0gMFxyXG4gICAgICAgICAgbWFpblpvb20uc2NhbGUobGFzdFpvb21TY2FsZSkudHJhbnNsYXRlKGxhc3RQb3NpdGlvbilcclxuICAgICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGxhc3RQb3NpdGlvbiArIFwiKSBzY2FsZShcIiArIGxhc3Rab29tU2NhbGUgKyBcIilcIilcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShuZXdTY2FsZSkudHJhbnNsYXRlKFt4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0XSlcclxuICAgICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHhDZW50ZXJPZmZzZXQgKyBcIiwgXCIgKyB5Q2VudGVyT2Zmc2V0ICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKVxyXG5cclxuICAgICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgLT5cclxuICAgICAgICAgIGV2ID0gZDMuZXZlbnRcclxuICAgICAgICAgIGxhc3Rab29tU2NhbGUgPSBldi5zY2FsZVxyXG4gICAgICAgICAgbGFzdFBvc2l0aW9uID0gZXYudHJhbnNsYXRlXHJcbiAgICAgICAgICBkM21haW5TdmdHLmF0dHIgXCJ0cmFuc2Zvcm1cIiwgXCJ0cmFuc2xhdGUoXCIgKyBsYXN0UG9zaXRpb24gKyBcIikgc2NhbGUoXCIgKyBsYXN0Wm9vbVNjYWxlICsgXCIpXCJcclxuICAgICAgICApXHJcbiAgICAgICAgbWFpblpvb20oZDNtYWluU3ZnKVxyXG5cclxuICAgICAgICBkM21haW5TdmdHLnNlbGVjdEFsbCgnLm5vZGUnKS5vbiAnY2xpY2snLCAoZCkgLT5cclxuICAgICAgICAgIHNjb3BlLnNldE5vZGUoeyBub2RlaWQ6IGQgfSlcclxuXHJcbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMucGxhbiwgKG5ld1BsYW4pIC0+XHJcbiAgICAgIGRyYXdHcmFwaCgpIGlmIG5ld1BsYW5cclxuXHJcbiAgICBzY29wZS4kd2F0Y2ggYXR0cnMud2F0ZXJtYXJrcywgKG5ld1dhdGVybWFya3MpIC0+XHJcbiAgICAgIGRyYXdHcmFwaCgpIGlmIG5ld1dhdGVybWFya3MgJiYgc2NvcGUucGxhblxyXG5cclxuICAgIHJldHVyblxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ3ZlcnRleCcsIGZ1bmN0aW9uKCRzdGF0ZSkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiBcIjxzdmcgY2xhc3M9J3RpbWVsaW5lIHNlY29uZGFyeScgd2lkdGg9JzAnIGhlaWdodD0nMCc+PC9zdmc+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIGRhdGE6IFwiPVwiXG4gICAgfSxcbiAgICBsaW5rOiBmdW5jdGlvbihzY29wZSwgZWxlbSwgYXR0cnMpIHtcbiAgICAgIHZhciBhbmFseXplVGltZSwgY29udGFpbmVyVywgc3ZnRWw7XG4gICAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyk7XG4gICAgICBhbmFseXplVGltZSA9IGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIGNoYXJ0LCBzdmcsIHRlc3REYXRhO1xuICAgICAgICBkMy5zZWxlY3Qoc3ZnRWwpLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKCk7XG4gICAgICAgIHRlc3REYXRhID0gW107XG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnN1YnRhc2tzLCBmdW5jdGlvbihzdWJ0YXNrLCBpKSB7XG4gICAgICAgICAgdmFyIHRpbWVzO1xuICAgICAgICAgIHRpbWVzID0gW1xuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBsYWJlbDogXCJTY2hlZHVsZWRcIixcbiAgICAgICAgICAgICAgY29sb3I6IFwiIzY2NlwiLFxuICAgICAgICAgICAgICBib3JkZXJDb2xvcjogXCIjNTU1XCIsXG4gICAgICAgICAgICAgIHN0YXJ0aW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIlNDSEVEVUxFRFwiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkRFUExPWUlOR1wiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9LCB7XG4gICAgICAgICAgICAgIGxhYmVsOiBcIkRlcGxveWluZ1wiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjYWFhXCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiREVQTE9ZSU5HXCJdLFxuICAgICAgICAgICAgICBlbmRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgdHlwZTogJ3JlZ3VsYXInXG4gICAgICAgICAgICB9XG4gICAgICAgICAgXTtcbiAgICAgICAgICBpZiAoc3VidGFzay50aW1lc3RhbXBzW1wiRklOSVNIRURcIl0gPiAwKSB7XG4gICAgICAgICAgICB0aW1lcy5wdXNoKHtcbiAgICAgICAgICAgICAgbGFiZWw6IFwiUnVubmluZ1wiLFxuICAgICAgICAgICAgICBjb2xvcjogXCIjZGRkXCIsXG4gICAgICAgICAgICAgIGJvcmRlckNvbG9yOiBcIiM1NTVcIixcbiAgICAgICAgICAgICAgc3RhcnRpbmdfdGltZTogc3VidGFzay50aW1lc3RhbXBzW1wiUlVOTklOR1wiXSxcbiAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHN1YnRhc2sudGltZXN0YW1wc1tcIkZJTklTSEVEXCJdLFxuICAgICAgICAgICAgICB0eXBlOiAncmVndWxhcidcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgICByZXR1cm4gdGVzdERhdGEucHVzaCh7XG4gICAgICAgICAgICBsYWJlbDogXCIoXCIgKyBzdWJ0YXNrLnN1YnRhc2sgKyBcIikgXCIgKyBzdWJ0YXNrLmhvc3QsXG4gICAgICAgICAgICB0aW1lczogdGltZXNcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSk7XG4gICAgICAgIGNoYXJ0ID0gZDMudGltZWxpbmUoKS5zdGFjaygpLnRpY2tGb3JtYXQoe1xuICAgICAgICAgIGZvcm1hdDogZDMudGltZS5mb3JtYXQoXCIlTFwiKSxcbiAgICAgICAgICB0aWNrU2l6ZTogMVxuICAgICAgICB9KS5wcmVmaXgoXCJzaW5nbGVcIikubGFiZWxGb3JtYXQoZnVuY3Rpb24obGFiZWwpIHtcbiAgICAgICAgICByZXR1cm4gbGFiZWw7XG4gICAgICAgIH0pLm1hcmdpbih7XG4gICAgICAgICAgbGVmdDogMTAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSkuaXRlbUhlaWdodCgzMCkucmVsYXRpdmVUaW1lKCk7XG4gICAgICAgIHJldHVybiBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBhbmFseXplVGltZShzY29wZS5kYXRhKTtcbiAgICB9XG4gIH07XG59KS5kaXJlY3RpdmUoJ3RpbWVsaW5lJywgZnVuY3Rpb24oJHN0YXRlKSB7XG4gIHJldHVybiB7XG4gICAgdGVtcGxhdGU6IFwiPHN2ZyBjbGFzcz0ndGltZWxpbmUnIHdpZHRoPScwJyBoZWlnaHQ9JzAnPjwvc3ZnPlwiLFxuICAgIHNjb3BlOiB7XG4gICAgICB2ZXJ0aWNlczogXCI9XCIsXG4gICAgICBqb2JpZDogXCI9XCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtLCBhdHRycykge1xuICAgICAgdmFyIGFuYWx5emVUaW1lLCBjb250YWluZXJXLCBzdmdFbCwgdHJhbnNsYXRlTGFiZWw7XG4gICAgICBzdmdFbCA9IGVsZW0uY2hpbGRyZW4oKVswXTtcbiAgICAgIGNvbnRhaW5lclcgPSBlbGVtLndpZHRoKCk7XG4gICAgICBhbmd1bGFyLmVsZW1lbnQoc3ZnRWwpLmF0dHIoJ3dpZHRoJywgY29udGFpbmVyVyk7XG4gICAgICB0cmFuc2xhdGVMYWJlbCA9IGZ1bmN0aW9uKGxhYmVsKSB7XG4gICAgICAgIHJldHVybiBsYWJlbC5yZXBsYWNlKFwiJmd0O1wiLCBcIj5cIik7XG4gICAgICB9O1xuICAgICAgYW5hbHl6ZVRpbWUgPSBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHZhciBjaGFydCwgc3ZnLCB0ZXN0RGF0YTtcbiAgICAgICAgZDMuc2VsZWN0KHN2Z0VsKS5zZWxlY3RBbGwoXCIqXCIpLnJlbW92ZSgpO1xuICAgICAgICB0ZXN0RGF0YSA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YSwgZnVuY3Rpb24odmVydGV4KSB7XG4gICAgICAgICAgaWYgKHZlcnRleFsnc3RhcnQtdGltZSddID4gLTEpIHtcbiAgICAgICAgICAgIGlmICh2ZXJ0ZXgudHlwZSA9PT0gJ3NjaGVkdWxlZCcpIHtcbiAgICAgICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSksXG4gICAgICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNjY2NjY2NcIixcbiAgICAgICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzU1NTU1NVwiLFxuICAgICAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgdHlwZTogdmVydGV4LnR5cGVcbiAgICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBdXG4gICAgICAgICAgICAgIH0pO1xuICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgcmV0dXJuIHRlc3REYXRhLnB1c2goe1xuICAgICAgICAgICAgICAgIHRpbWVzOiBbXG4gICAgICAgICAgICAgICAgICB7XG4gICAgICAgICAgICAgICAgICAgIGxhYmVsOiB0cmFuc2xhdGVMYWJlbCh2ZXJ0ZXgubmFtZSksXG4gICAgICAgICAgICAgICAgICAgIGNvbG9yOiBcIiNkOWYxZjdcIixcbiAgICAgICAgICAgICAgICAgICAgYm9yZGVyQ29sb3I6IFwiIzYyY2RlYVwiLFxuICAgICAgICAgICAgICAgICAgICBzdGFydGluZ190aW1lOiB2ZXJ0ZXhbJ3N0YXJ0LXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgZW5kaW5nX3RpbWU6IHZlcnRleFsnZW5kLXRpbWUnXSxcbiAgICAgICAgICAgICAgICAgICAgbGluazogdmVydGV4LmlkLFxuICAgICAgICAgICAgICAgICAgICB0eXBlOiB2ZXJ0ZXgudHlwZVxuICAgICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIF1cbiAgICAgICAgICAgICAgfSk7XG4gICAgICAgICAgICB9XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgICAgY2hhcnQgPSBkMy50aW1lbGluZSgpLnN0YWNrKCkuY2xpY2soZnVuY3Rpb24oZCwgaSwgZGF0dW0pIHtcbiAgICAgICAgICBpZiAoZC5saW5rKSB7XG4gICAgICAgICAgICByZXR1cm4gJHN0YXRlLmdvKFwic2luZ2xlLWpvYi50aW1lbGluZS52ZXJ0ZXhcIiwge1xuICAgICAgICAgICAgICBqb2JpZDogc2NvcGUuam9iaWQsXG4gICAgICAgICAgICAgIHZlcnRleElkOiBkLmxpbmtcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH1cbiAgICAgICAgfSkudGlja0Zvcm1hdCh7XG4gICAgICAgICAgZm9ybWF0OiBkMy50aW1lLmZvcm1hdChcIiVMXCIpLFxuICAgICAgICAgIHRpY2tTaXplOiAxXG4gICAgICAgIH0pLnByZWZpeChcIm1haW5cIikubWFyZ2luKHtcbiAgICAgICAgICBsZWZ0OiAwLFxuICAgICAgICAgIHJpZ2h0OiAwLFxuICAgICAgICAgIHRvcDogMCxcbiAgICAgICAgICBib3R0b206IDBcbiAgICAgICAgfSkuaXRlbUhlaWdodCgzMCkuc2hvd0JvcmRlckxpbmUoKS5zaG93SG91clRpbWVsaW5lKCk7XG4gICAgICAgIHJldHVybiBzdmcgPSBkMy5zZWxlY3Qoc3ZnRWwpLmRhdHVtKHRlc3REYXRhKS5jYWxsKGNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBzY29wZS4kd2F0Y2goYXR0cnMudmVydGljZXMsIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgaWYgKGRhdGEpIHtcbiAgICAgICAgICByZXR1cm4gYW5hbHl6ZVRpbWUoZGF0YSk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbn0pLmRpcmVjdGl2ZSgnc3BsaXQnLCBmdW5jdGlvbigpIHtcbiAgcmV0dXJuIHtcbiAgICBjb21waWxlOiBmdW5jdGlvbih0RWxlbSwgdEF0dHJzKSB7XG4gICAgICByZXR1cm4gU3BsaXQodEVsZW0uY2hpbGRyZW4oKSwge1xuICAgICAgICBzaXplczogWzUwLCA1MF0sXG4gICAgICAgIGRpcmVjdGlvbjogJ3ZlcnRpY2FsJ1xuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSkuZGlyZWN0aXZlKCdqb2JQbGFuJywgZnVuY3Rpb24oJHRpbWVvdXQpIHtcbiAgcmV0dXJuIHtcbiAgICB0ZW1wbGF0ZTogXCI8c3ZnIGNsYXNzPSdncmFwaCc+PGcgLz48L3N2Zz4gPHN2ZyBjbGFzcz0ndG1wJyB3aWR0aD0nMScgaGVpZ2h0PScxJz48ZyAvPjwvc3ZnPiA8ZGl2IGNsYXNzPSdidG4tZ3JvdXAgem9vbS1idXR0b25zJz4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLWluJyBuZy1jbGljaz0nem9vbUluKCknPjxpIGNsYXNzPSdmYSBmYS1wbHVzJyAvPjwvYT4gPGEgY2xhc3M9J2J0biBidG4tZGVmYXVsdCB6b29tLW91dCcgbmctY2xpY2s9J3pvb21PdXQoKSc+PGkgY2xhc3M9J2ZhIGZhLW1pbnVzJyAvPjwvYT4gPC9kaXY+XCIsXG4gICAgc2NvcGU6IHtcbiAgICAgIHBsYW46ICc9JyxcbiAgICAgIHdhdGVybWFya3M6ICc9JyxcbiAgICAgIHNldE5vZGU6ICcmJ1xuICAgIH0sXG4gICAgbGluazogZnVuY3Rpb24oc2NvcGUsIGVsZW0sIGF0dHJzKSB7XG4gICAgICB2YXIgY29udGFpbmVyVywgY3JlYXRlRWRnZSwgY3JlYXRlTGFiZWxFZGdlLCBjcmVhdGVMYWJlbE5vZGUsIGNyZWF0ZU5vZGUsIGQzbWFpblN2ZywgZDNtYWluU3ZnRywgZDN0bXBTdmcsIGRyYXdHcmFwaCwgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uLCBnLCBnZXROb2RlVHlwZSwgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSwgam9iaWQsIGxhc3RQb3NpdGlvbiwgbGFzdFpvb21TY2FsZSwgbG9hZEpzb25Ub0RhZ3JlLCBtYWluRywgbWFpblN2Z0VsZW1lbnQsIG1haW5UbXBFbGVtZW50LCBtYWluWm9vbSwgbWVyZ2VXYXRlcm1hcmtzLCBzZWFyY2hGb3JOb2RlLCBzaG9ydGVuU3RyaW5nLCBzdWJncmFwaHM7XG4gICAgICBnID0gbnVsbDtcbiAgICAgIG1haW5ab29tID0gZDMuYmVoYXZpb3Iuem9vbSgpO1xuICAgICAgc3ViZ3JhcGhzID0gW107XG4gICAgICBqb2JpZCA9IGF0dHJzLmpvYmlkO1xuICAgICAgbWFpblN2Z0VsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMF07XG4gICAgICBtYWluRyA9IGVsZW0uY2hpbGRyZW4oKS5jaGlsZHJlbigpWzBdO1xuICAgICAgbWFpblRtcEVsZW1lbnQgPSBlbGVtLmNoaWxkcmVuKClbMV07XG4gICAgICBkM21haW5TdmcgPSBkMy5zZWxlY3QobWFpblN2Z0VsZW1lbnQpO1xuICAgICAgZDNtYWluU3ZnRyA9IGQzLnNlbGVjdChtYWluRyk7XG4gICAgICBkM3RtcFN2ZyA9IGQzLnNlbGVjdChtYWluVG1wRWxlbWVudCk7XG4gICAgICBjb250YWluZXJXID0gZWxlbS53aWR0aCgpO1xuICAgICAgYW5ndWxhci5lbGVtZW50KGVsZW0uY2hpbGRyZW4oKVswXSkud2lkdGgoY29udGFpbmVyVyk7XG4gICAgICBsYXN0Wm9vbVNjYWxlID0gMDtcbiAgICAgIGxhc3RQb3NpdGlvbiA9IDA7XG4gICAgICBzY29wZS56b29tSW4gPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHRyYW5zbGF0ZSwgdjEsIHYyO1xuICAgICAgICBpZiAobWFpblpvb20uc2NhbGUoKSA8IDIuOTkpIHtcbiAgICAgICAgICB0cmFuc2xhdGUgPSBtYWluWm9vbS50cmFuc2xhdGUoKTtcbiAgICAgICAgICB2MSA9IHRyYW5zbGF0ZVswXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICB2MiA9IHRyYW5zbGF0ZVsxXSAqIChtYWluWm9vbS5zY2FsZSgpICsgMC4xIC8gKG1haW5ab29tLnNjYWxlKCkpKTtcbiAgICAgICAgICBtYWluWm9vbS5zY2FsZShtYWluWm9vbS5zY2FsZSgpICsgMC4xKTtcbiAgICAgICAgICBtYWluWm9vbS50cmFuc2xhdGUoW3YxLCB2Ml0pO1xuICAgICAgICAgIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIHYxICsgXCIsXCIgKyB2MiArIFwiKSBzY2FsZShcIiArIG1haW5ab29tLnNjYWxlKCkgKyBcIilcIik7XG4gICAgICAgICAgbGFzdFpvb21TY2FsZSA9IG1haW5ab29tLnNjYWxlKCk7XG4gICAgICAgICAgcmV0dXJuIGxhc3RQb3NpdGlvbiA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuem9vbU91dCA9IGZ1bmN0aW9uKCkge1xuICAgICAgICB2YXIgdHJhbnNsYXRlLCB2MSwgdjI7XG4gICAgICAgIGlmIChtYWluWm9vbS5zY2FsZSgpID4gMC4zMSkge1xuICAgICAgICAgIG1haW5ab29tLnNjYWxlKG1haW5ab29tLnNjYWxlKCkgLSAwLjEpO1xuICAgICAgICAgIHRyYW5zbGF0ZSA9IG1haW5ab29tLnRyYW5zbGF0ZSgpO1xuICAgICAgICAgIHYxID0gdHJhbnNsYXRlWzBdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIHYyID0gdHJhbnNsYXRlWzFdICogKG1haW5ab29tLnNjYWxlKCkgLSAwLjEgLyAobWFpblpvb20uc2NhbGUoKSkpO1xuICAgICAgICAgIG1haW5ab29tLnRyYW5zbGF0ZShbdjEsIHYyXSk7XG4gICAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgdjEgKyBcIixcIiArIHYyICsgXCIpIHNjYWxlKFwiICsgbWFpblpvb20uc2NhbGUoKSArIFwiKVwiKTtcbiAgICAgICAgICBsYXN0Wm9vbVNjYWxlID0gbWFpblpvb20uc2NhbGUoKTtcbiAgICAgICAgICByZXR1cm4gbGFzdFBvc2l0aW9uID0gbWFpblpvb20udHJhbnNsYXRlKCk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVMYWJlbEVkZ2UgPSBmdW5jdGlvbihlbCkge1xuICAgICAgICB2YXIgbGFiZWxWYWx1ZTtcbiAgICAgICAgbGFiZWxWYWx1ZSA9IFwiXCI7XG4gICAgICAgIGlmICgoZWwuc2hpcF9zdHJhdGVneSAhPSBudWxsKSB8fCAoZWwubG9jYWxfc3RyYXRlZ3kgIT0gbnVsbCkpIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGRpdiBjbGFzcz0nZWRnZS1sYWJlbCc+XCI7XG4gICAgICAgICAgaWYgKGVsLnNoaXBfc3RyYXRlZ3kgIT0gbnVsbCkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBlbC5zaGlwX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwudGVtcF9tb2RlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCIgKFwiICsgZWwudGVtcF9tb2RlICsgXCIpXCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5sb2NhbF9zdHJhdGVneSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiLDxicj5cIiArIGVsLmxvY2FsX3N0cmF0ZWd5O1xuICAgICAgICAgIH1cbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPC9kaXY+XCI7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgaXNTcGVjaWFsSXRlcmF0aW9uTm9kZSA9IGZ1bmN0aW9uKGluZm8pIHtcbiAgICAgICAgcmV0dXJuIGluZm8gPT09IFwicGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIgfHwgaW5mbyA9PT0gXCJ3b3Jrc2V0XCIgfHwgaW5mbyA9PT0gXCJuZXh0V29ya3NldFwiIHx8IGluZm8gPT09IFwic29sdXRpb25TZXRcIiB8fCBpbmZvID09PSBcInNvbHV0aW9uRGVsdGFcIjtcbiAgICAgIH07XG4gICAgICBnZXROb2RlVHlwZSA9IGZ1bmN0aW9uKGVsLCBpbmZvKSB7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgcmV0dXJuICdub2RlLW1pcnJvcic7XG4gICAgICAgIH0gZWxzZSBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1pdGVyYXRpb24nO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHJldHVybiAnbm9kZS1ub3JtYWwnO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgY3JlYXRlTGFiZWxOb2RlID0gZnVuY3Rpb24oZWwsIGluZm8sIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN0ZXBOYW1lO1xuICAgICAgICBsYWJlbFZhbHVlID0gXCI8ZGl2IGhyZWY9JyMvam9icy9cIiArIGpvYmlkICsgXCIvdmVydGV4L1wiICsgZWwuaWQgKyBcIicgY2xhc3M9J25vZGUtbGFiZWwgXCIgKyBnZXROb2RlVHlwZShlbCwgaW5mbykgKyBcIic+XCI7XG4gICAgICAgIGlmIChpbmZvID09PSBcIm1pcnJvclwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoMyBjbGFzcz0nbm9kZS1uYW1lJz5NaXJyb3Igb2YgXCIgKyBlbC5vcGVyYXRvciArIFwiPC9oMz5cIjtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGgzIGNsYXNzPSdub2RlLW5hbWUnPlwiICsgZWwub3BlcmF0b3IgKyBcIjwvaDM+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLmRlc2NyaXB0aW9uID09PSBcIlwiKSB7XG4gICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIlwiO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHN0ZXBOYW1lID0gZWwuZGVzY3JpcHRpb247XG4gICAgICAgICAgc3RlcE5hbWUgPSBzaG9ydGVuU3RyaW5nKHN0ZXBOYW1lKTtcbiAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg0IGNsYXNzPSdzdGVwLW5hbWUnPlwiICsgc3RlcE5hbWUgKyBcIjwvaDQ+XCI7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24gIT0gbnVsbCkge1xuICAgICAgICAgIGxhYmVsVmFsdWUgKz0gZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uKGVsLmlkLCBtYXhXLCBtYXhIKTtcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBpZiAoaXNTcGVjaWFsSXRlcmF0aW9uTm9kZShpbmZvKSkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5cIiArIGluZm8gKyBcIiBOb2RlPC9oNT5cIjtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGVsLnBhcmFsbGVsaXNtICE9PSBcIlwiKSB7XG4gICAgICAgICAgICBsYWJlbFZhbHVlICs9IFwiPGg1PlBhcmFsbGVsaXNtOiBcIiArIGVsLnBhcmFsbGVsaXNtICsgXCI8L2g1PlwiO1xuICAgICAgICAgIH1cbiAgICAgICAgICBpZiAoZWwubG93V2F0ZXJtYXJrICE9PSB1bmRlZmluZWQpIHtcbiAgICAgICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8aDU+TG93IFdhdGVybWFyazogXCIgKyBlbC5sb3dXYXRlcm1hcmsgKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmICghKGVsLm9wZXJhdG9yID09PSB1bmRlZmluZWQgfHwgIWVsLm9wZXJhdG9yX3N0cmF0ZWd5KSkge1xuICAgICAgICAgICAgbGFiZWxWYWx1ZSArPSBcIjxoNT5PcGVyYXRpb246IFwiICsgc2hvcnRlblN0cmluZyhlbC5vcGVyYXRvcl9zdHJhdGVneSkgKyBcIjwvaDU+XCI7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGxhYmVsVmFsdWUgKz0gXCI8L2Rpdj5cIjtcbiAgICAgICAgcmV0dXJuIGxhYmVsVmFsdWU7XG4gICAgICB9O1xuICAgICAgZXh0ZW5kTGFiZWxOb2RlRm9ySXRlcmF0aW9uID0gZnVuY3Rpb24oaWQsIG1heFcsIG1heEgpIHtcbiAgICAgICAgdmFyIGxhYmVsVmFsdWUsIHN2Z0lEO1xuICAgICAgICBzdmdJRCA9IFwic3ZnLVwiICsgaWQ7XG4gICAgICAgIGxhYmVsVmFsdWUgPSBcIjxzdmcgY2xhc3M9J1wiICsgc3ZnSUQgKyBcIicgd2lkdGg9XCIgKyBtYXhXICsgXCIgaGVpZ2h0PVwiICsgbWF4SCArIFwiPjxnIC8+PC9zdmc+XCI7XG4gICAgICAgIHJldHVybiBsYWJlbFZhbHVlO1xuICAgICAgfTtcbiAgICAgIHNob3J0ZW5TdHJpbmcgPSBmdW5jdGlvbihzKSB7XG4gICAgICAgIHZhciBzYnI7XG4gICAgICAgIGlmIChzLmNoYXJBdCgwKSA9PT0gXCI8XCIpIHtcbiAgICAgICAgICBzID0gcy5yZXBsYWNlKFwiPFwiLCBcIiZsdDtcIik7XG4gICAgICAgICAgcyA9IHMucmVwbGFjZShcIj5cIiwgXCImZ3Q7XCIpO1xuICAgICAgICB9XG4gICAgICAgIHNiciA9IFwiXCI7XG4gICAgICAgIHdoaWxlIChzLmxlbmd0aCA+IDMwKSB7XG4gICAgICAgICAgc2JyID0gc2JyICsgcy5zdWJzdHJpbmcoMCwgMzApICsgXCI8YnI+XCI7XG4gICAgICAgICAgcyA9IHMuc3Vic3RyaW5nKDMwLCBzLmxlbmd0aCk7XG4gICAgICAgIH1cbiAgICAgICAgc2JyID0gc2JyICsgcztcbiAgICAgICAgcmV0dXJuIHNicjtcbiAgICAgIH07XG4gICAgICBjcmVhdGVOb2RlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGlzUGFyZW50LCBtYXhXLCBtYXhIKSB7XG4gICAgICAgIGlmIChpc1BhcmVudCA9PSBudWxsKSB7XG4gICAgICAgICAgaXNQYXJlbnQgPSBmYWxzZTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoZWwuaWQgPT09IGRhdGEucGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwicGFydGlhbFNvbHV0aW9uXCIsIG1heFcsIG1heEgpLFxuICAgICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgICBcImNsYXNzXCI6IGdldE5vZGVUeXBlKGVsLCBcInBhcnRpYWxTb2x1dGlvblwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLm5leHRfcGFydGlhbF9zb2x1dGlvbikge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwibmV4dFBhcnRpYWxTb2x1dGlvblwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0UGFydGlhbFNvbHV0aW9uXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEud29ya3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwid29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJ3b3Jrc2V0XCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSBpZiAoZWwuaWQgPT09IGRhdGEubmV4dF93b3Jrc2V0KSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJuZXh0V29ya3NldFwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJuZXh0V29ya3NldFwiKVxuICAgICAgICAgIH0pO1xuICAgICAgICB9IGVsc2UgaWYgKGVsLmlkID09PSBkYXRhLnNvbHV0aW9uX3NldCkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25TZXRcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwic29sdXRpb25TZXRcIilcbiAgICAgICAgICB9KTtcbiAgICAgICAgfSBlbHNlIGlmIChlbC5pZCA9PT0gZGF0YS5zb2x1dGlvbl9kZWx0YSkge1xuICAgICAgICAgIHJldHVybiBnLnNldE5vZGUoZWwuaWQsIHtcbiAgICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbE5vZGUoZWwsIFwic29sdXRpb25EZWx0YVwiLCBtYXhXLCBtYXhIKSxcbiAgICAgICAgICAgIGxhYmVsVHlwZTogJ2h0bWwnLFxuICAgICAgICAgICAgXCJjbGFzc1wiOiBnZXROb2RlVHlwZShlbCwgXCJzb2x1dGlvbkRlbHRhXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgcmV0dXJuIGcuc2V0Tm9kZShlbC5pZCwge1xuICAgICAgICAgICAgbGFiZWw6IGNyZWF0ZUxhYmVsTm9kZShlbCwgXCJcIiwgbWF4VywgbWF4SCksXG4gICAgICAgICAgICBsYWJlbFR5cGU6ICdodG1sJyxcbiAgICAgICAgICAgIFwiY2xhc3NcIjogZ2V0Tm9kZVR5cGUoZWwsIFwiXCIpXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBjcmVhdGVFZGdlID0gZnVuY3Rpb24oZywgZGF0YSwgZWwsIGV4aXN0aW5nTm9kZXMsIHByZWQpIHtcbiAgICAgICAgcmV0dXJuIGcuc2V0RWRnZShwcmVkLmlkLCBlbC5pZCwge1xuICAgICAgICAgIGxhYmVsOiBjcmVhdGVMYWJlbEVkZ2UocHJlZCksXG4gICAgICAgICAgbGFiZWxUeXBlOiAnaHRtbCcsXG4gICAgICAgICAgYXJyb3doZWFkOiAnbm9ybWFsJ1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgICBsb2FkSnNvblRvRGFncmUgPSBmdW5jdGlvbihnLCBkYXRhKSB7XG4gICAgICAgIHZhciBlbCwgZXhpc3RpbmdOb2RlcywgaXNQYXJlbnQsIGssIGwsIGxlbiwgbGVuMSwgbWF4SCwgbWF4VywgcHJlZCwgciwgcmVmLCBzZywgdG9JdGVyYXRlO1xuICAgICAgICBleGlzdGluZ05vZGVzID0gW107XG4gICAgICAgIGlmIChkYXRhLm5vZGVzICE9IG51bGwpIHtcbiAgICAgICAgICB0b0l0ZXJhdGUgPSBkYXRhLm5vZGVzO1xuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIHRvSXRlcmF0ZSA9IGRhdGEuc3RlcF9mdW5jdGlvbjtcbiAgICAgICAgICBpc1BhcmVudCA9IHRydWU7XG4gICAgICAgIH1cbiAgICAgICAgZm9yIChrID0gMCwgbGVuID0gdG9JdGVyYXRlLmxlbmd0aDsgayA8IGxlbjsgaysrKSB7XG4gICAgICAgICAgZWwgPSB0b0l0ZXJhdGVba107XG4gICAgICAgICAgbWF4VyA9IDA7XG4gICAgICAgICAgbWF4SCA9IDA7XG4gICAgICAgICAgaWYgKGVsLnN0ZXBfZnVuY3Rpb24pIHtcbiAgICAgICAgICAgIHNnID0gbmV3IGRhZ3JlRDMuZ3JhcGhsaWIuR3JhcGgoe1xuICAgICAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgICAgICBjb21wb3VuZDogdHJ1ZVxuICAgICAgICAgICAgfSkuc2V0R3JhcGgoe1xuICAgICAgICAgICAgICBub2Rlc2VwOiAyMCxcbiAgICAgICAgICAgICAgZWRnZXNlcDogMCxcbiAgICAgICAgICAgICAgcmFua3NlcDogMjAsXG4gICAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICAgICAgbWFyZ2lueDogMTAsXG4gICAgICAgICAgICAgIG1hcmdpbnk6IDEwXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICAgIHN1YmdyYXBoc1tlbC5pZF0gPSBzZztcbiAgICAgICAgICAgIGxvYWRKc29uVG9EYWdyZShzZywgZWwpO1xuICAgICAgICAgICAgciA9IG5ldyBkYWdyZUQzLnJlbmRlcigpO1xuICAgICAgICAgICAgZDN0bXBTdmcuc2VsZWN0KCdnJykuY2FsbChyLCBzZyk7XG4gICAgICAgICAgICBtYXhXID0gc2cuZ3JhcGgoKS53aWR0aDtcbiAgICAgICAgICAgIG1heEggPSBzZy5ncmFwaCgpLmhlaWdodDtcbiAgICAgICAgICAgIGFuZ3VsYXIuZWxlbWVudChtYWluVG1wRWxlbWVudCkuZW1wdHkoKTtcbiAgICAgICAgICB9XG4gICAgICAgICAgY3JlYXRlTm9kZShnLCBkYXRhLCBlbCwgaXNQYXJlbnQsIG1heFcsIG1heEgpO1xuICAgICAgICAgIGV4aXN0aW5nTm9kZXMucHVzaChlbC5pZCk7XG4gICAgICAgICAgaWYgKGVsLmlucHV0cyAhPSBudWxsKSB7XG4gICAgICAgICAgICByZWYgPSBlbC5pbnB1dHM7XG4gICAgICAgICAgICBmb3IgKGwgPSAwLCBsZW4xID0gcmVmLmxlbmd0aDsgbCA8IGxlbjE7IGwrKykge1xuICAgICAgICAgICAgICBwcmVkID0gcmVmW2xdO1xuICAgICAgICAgICAgICBjcmVhdGVFZGdlKGcsIGRhdGEsIGVsLCBleGlzdGluZ05vZGVzLCBwcmVkKTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGc7XG4gICAgICB9O1xuICAgICAgc2VhcmNoRm9yTm9kZSA9IGZ1bmN0aW9uKGRhdGEsIG5vZGVJRCkge1xuICAgICAgICB2YXIgZWwsIGksIGo7XG4gICAgICAgIGZvciAoaSBpbiBkYXRhLm5vZGVzKSB7XG4gICAgICAgICAgZWwgPSBkYXRhLm5vZGVzW2ldO1xuICAgICAgICAgIGlmIChlbC5pZCA9PT0gbm9kZUlEKSB7XG4gICAgICAgICAgICByZXR1cm4gZWw7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uICE9IG51bGwpIHtcbiAgICAgICAgICAgIGZvciAoaiBpbiBlbC5zdGVwX2Z1bmN0aW9uKSB7XG4gICAgICAgICAgICAgIGlmIChlbC5zdGVwX2Z1bmN0aW9uW2pdLmlkID09PSBub2RlSUQpIHtcbiAgICAgICAgICAgICAgICByZXR1cm4gZWwuc3RlcF9mdW5jdGlvbltqXTtcbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIG1lcmdlV2F0ZXJtYXJrcyA9IGZ1bmN0aW9uKGRhdGEsIHdhdGVybWFya3MpIHtcbiAgICAgICAgdmFyIGssIGxlbiwgbm9kZSwgcmVmO1xuICAgICAgICBpZiAoIV8uaXNFbXB0eSh3YXRlcm1hcmtzKSkge1xuICAgICAgICAgIHJlZiA9IGRhdGEubm9kZXM7XG4gICAgICAgICAgZm9yIChrID0gMCwgbGVuID0gcmVmLmxlbmd0aDsgayA8IGxlbjsgaysrKSB7XG4gICAgICAgICAgICBub2RlID0gcmVmW2tdO1xuICAgICAgICAgICAgaWYgKHdhdGVybWFya3Nbbm9kZS5pZF0gJiYgIWlzTmFOKHdhdGVybWFya3Nbbm9kZS5pZF1bXCJsb3dXYXRlcm1hcmtcIl0pKSB7XG4gICAgICAgICAgICAgIG5vZGUubG93V2F0ZXJtYXJrID0gd2F0ZXJtYXJrc1tub2RlLmlkXVtcImxvd1dhdGVybWFya1wiXTtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIGRhdGE7XG4gICAgICB9O1xuICAgICAgbGFzdFBvc2l0aW9uID0gMDtcbiAgICAgIGxhc3Rab29tU2NhbGUgPSAwO1xuICAgICAgZHJhd0dyYXBoID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHZhciBpLCBuZXdTY2FsZSwgcmVuZGVyZXIsIHNnLCB4Q2VudGVyT2Zmc2V0LCB5Q2VudGVyT2Zmc2V0O1xuICAgICAgICBpZiAoc2NvcGUucGxhbikge1xuICAgICAgICAgIGcgPSBuZXcgZGFncmVEMy5ncmFwaGxpYi5HcmFwaCh7XG4gICAgICAgICAgICBtdWx0aWdyYXBoOiB0cnVlLFxuICAgICAgICAgICAgY29tcG91bmQ6IHRydWVcbiAgICAgICAgICB9KS5zZXRHcmFwaCh7XG4gICAgICAgICAgICBub2Rlc2VwOiA3MCxcbiAgICAgICAgICAgIGVkZ2VzZXA6IDAsXG4gICAgICAgICAgICByYW5rc2VwOiA1MCxcbiAgICAgICAgICAgIHJhbmtkaXI6IFwiTFJcIixcbiAgICAgICAgICAgIG1hcmdpbng6IDQwLFxuICAgICAgICAgICAgbWFyZ2lueTogNDBcbiAgICAgICAgICB9KTtcbiAgICAgICAgICBsb2FkSnNvblRvRGFncmUoZywgbWVyZ2VXYXRlcm1hcmtzKHNjb3BlLnBsYW4sIHNjb3BlLndhdGVybWFya3MpKTtcbiAgICAgICAgICBkM21haW5TdmdHLnNlbGVjdEFsbChcIipcIikucmVtb3ZlKCk7XG4gICAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwic2NhbGUoXCIgKyAxICsgXCIpXCIpO1xuICAgICAgICAgIHJlbmRlcmVyID0gbmV3IGRhZ3JlRDMucmVuZGVyKCk7XG4gICAgICAgICAgZDNtYWluU3ZnRy5jYWxsKHJlbmRlcmVyLCBnKTtcbiAgICAgICAgICBmb3IgKGkgaW4gc3ViZ3JhcGhzKSB7XG4gICAgICAgICAgICBzZyA9IHN1YmdyYXBoc1tpXTtcbiAgICAgICAgICAgIGQzbWFpblN2Zy5zZWxlY3QoJ3N2Zy5zdmctJyArIGkgKyAnIGcnKS5jYWxsKHJlbmRlcmVyLCBzZyk7XG4gICAgICAgICAgfVxuICAgICAgICAgIG5ld1NjYWxlID0gMC41O1xuICAgICAgICAgIHhDZW50ZXJPZmZzZXQgPSBNYXRoLmZsb29yKChhbmd1bGFyLmVsZW1lbnQobWFpblN2Z0VsZW1lbnQpLndpZHRoKCkgLSBnLmdyYXBoKCkud2lkdGggKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgICB5Q2VudGVyT2Zmc2V0ID0gTWF0aC5mbG9vcigoYW5ndWxhci5lbGVtZW50KG1haW5TdmdFbGVtZW50KS5oZWlnaHQoKSAtIGcuZ3JhcGgoKS5oZWlnaHQgKiBuZXdTY2FsZSkgLyAyKTtcbiAgICAgICAgICBpZiAobGFzdFpvb21TY2FsZSAhPT0gMCAmJiBsYXN0UG9zaXRpb24gIT09IDApIHtcbiAgICAgICAgICAgIG1haW5ab29tLnNjYWxlKGxhc3Rab29tU2NhbGUpLnRyYW5zbGF0ZShsYXN0UG9zaXRpb24pO1xuICAgICAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgbGFzdFBvc2l0aW9uICsgXCIpIHNjYWxlKFwiICsgbGFzdFpvb21TY2FsZSArIFwiKVwiKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgbWFpblpvb20uc2NhbGUobmV3U2NhbGUpLnRyYW5zbGF0ZShbeENlbnRlck9mZnNldCwgeUNlbnRlck9mZnNldF0pO1xuICAgICAgICAgICAgZDNtYWluU3ZnRy5hdHRyKFwidHJhbnNmb3JtXCIsIFwidHJhbnNsYXRlKFwiICsgeENlbnRlck9mZnNldCArIFwiLCBcIiArIHlDZW50ZXJPZmZzZXQgKyBcIikgc2NhbGUoXCIgKyBtYWluWm9vbS5zY2FsZSgpICsgXCIpXCIpO1xuICAgICAgICAgIH1cbiAgICAgICAgICBtYWluWm9vbS5vbihcInpvb21cIiwgZnVuY3Rpb24oKSB7XG4gICAgICAgICAgICB2YXIgZXY7XG4gICAgICAgICAgICBldiA9IGQzLmV2ZW50O1xuICAgICAgICAgICAgbGFzdFpvb21TY2FsZSA9IGV2LnNjYWxlO1xuICAgICAgICAgICAgbGFzdFBvc2l0aW9uID0gZXYudHJhbnNsYXRlO1xuICAgICAgICAgICAgcmV0dXJuIGQzbWFpblN2Z0cuYXR0cihcInRyYW5zZm9ybVwiLCBcInRyYW5zbGF0ZShcIiArIGxhc3RQb3NpdGlvbiArIFwiKSBzY2FsZShcIiArIGxhc3Rab29tU2NhbGUgKyBcIilcIik7XG4gICAgICAgICAgfSk7XG4gICAgICAgICAgbWFpblpvb20oZDNtYWluU3ZnKTtcbiAgICAgICAgICByZXR1cm4gZDNtYWluU3ZnRy5zZWxlY3RBbGwoJy5ub2RlJykub24oJ2NsaWNrJywgZnVuY3Rpb24oZCkge1xuICAgICAgICAgICAgcmV0dXJuIHNjb3BlLnNldE5vZGUoe1xuICAgICAgICAgICAgICBub2RlaWQ6IGRcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLnBsYW4sIGZ1bmN0aW9uKG5ld1BsYW4pIHtcbiAgICAgICAgaWYgKG5ld1BsYW4pIHtcbiAgICAgICAgICByZXR1cm4gZHJhd0dyYXBoKCk7XG4gICAgICAgIH1cbiAgICAgIH0pO1xuICAgICAgc2NvcGUuJHdhdGNoKGF0dHJzLndhdGVybWFya3MsIGZ1bmN0aW9uKG5ld1dhdGVybWFya3MpIHtcbiAgICAgICAgaWYgKG5ld1dhdGVybWFya3MgJiYgc2NvcGUucGxhbikge1xuICAgICAgICAgIHJldHVybiBkcmF3R3JhcGgoKTtcbiAgICAgICAgfVxuICAgICAgfSk7XG4gICAgfVxuICB9O1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5zZXJ2aWNlICdKb2JzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRsb2csIGFtTW9tZW50LCAkcSwgJHRpbWVvdXQpIC0+XHJcbiAgY3VycmVudEpvYiA9IG51bGxcclxuICBjdXJyZW50UGxhbiA9IG51bGxcclxuXHJcbiAgZGVmZXJyZWRzID0ge31cclxuICBqb2JzID0ge1xyXG4gICAgcnVubmluZzogW11cclxuICAgIGZpbmlzaGVkOiBbXVxyXG4gICAgY2FuY2VsbGVkOiBbXVxyXG4gICAgZmFpbGVkOiBbXVxyXG4gIH1cclxuXHJcbiAgam9iT2JzZXJ2ZXJzID0gW11cclxuXHJcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gLT5cclxuICAgIGFuZ3VsYXIuZm9yRWFjaCBqb2JPYnNlcnZlcnMsIChjYWxsYmFjaykgLT5cclxuICAgICAgY2FsbGJhY2soKVxyXG5cclxuICBAcmVnaXN0ZXJPYnNlcnZlciA9IChjYWxsYmFjaykgLT5cclxuICAgIGpvYk9ic2VydmVycy5wdXNoKGNhbGxiYWNrKVxyXG5cclxuICBAdW5SZWdpc3Rlck9ic2VydmVyID0gKGNhbGxiYWNrKSAtPlxyXG4gICAgaW5kZXggPSBqb2JPYnNlcnZlcnMuaW5kZXhPZihjYWxsYmFjaylcclxuICAgIGpvYk9ic2VydmVycy5zcGxpY2UoaW5kZXgsIDEpXHJcblxyXG4gIEBzdGF0ZUxpc3QgPSAtPlxyXG4gICAgWyBcclxuICAgICAgIyAnQ1JFQVRFRCdcclxuICAgICAgJ1NDSEVEVUxFRCdcclxuICAgICAgJ0RFUExPWUlORydcclxuICAgICAgJ1JVTk5JTkcnXHJcbiAgICAgICdGSU5JU0hFRCdcclxuICAgICAgJ0ZBSUxFRCdcclxuICAgICAgJ0NBTkNFTElORydcclxuICAgICAgJ0NBTkNFTEVEJ1xyXG4gICAgXVxyXG5cclxuICBAdHJhbnNsYXRlTGFiZWxTdGF0ZSA9IChzdGF0ZSkgLT5cclxuICAgIHN3aXRjaCBzdGF0ZS50b0xvd2VyQ2FzZSgpXHJcbiAgICAgIHdoZW4gJ2ZpbmlzaGVkJyB0aGVuICdzdWNjZXNzJ1xyXG4gICAgICB3aGVuICdmYWlsZWQnIHRoZW4gJ2RhbmdlcidcclxuICAgICAgd2hlbiAnc2NoZWR1bGVkJyB0aGVuICdkZWZhdWx0J1xyXG4gICAgICB3aGVuICdkZXBsb3lpbmcnIHRoZW4gJ2luZm8nXHJcbiAgICAgIHdoZW4gJ3J1bm5pbmcnIHRoZW4gJ3ByaW1hcnknXHJcbiAgICAgIHdoZW4gJ2NhbmNlbGluZycgdGhlbiAnd2FybmluZydcclxuICAgICAgd2hlbiAncGVuZGluZycgdGhlbiAnaW5mbydcclxuICAgICAgd2hlbiAndG90YWwnIHRoZW4gJ2JsYWNrJ1xyXG4gICAgICBlbHNlICdkZWZhdWx0J1xyXG5cclxuICBAc2V0RW5kVGltZXMgPSAobGlzdCkgLT5cclxuICAgIGFuZ3VsYXIuZm9yRWFjaCBsaXN0LCAoaXRlbSwgam9iS2V5KSAtPlxyXG4gICAgICB1bmxlc3MgaXRlbVsnZW5kLXRpbWUnXSA+IC0xXHJcbiAgICAgICAgaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ11cclxuXHJcbiAgQHByb2Nlc3NWZXJ0aWNlcyA9IChkYXRhKSAtPlxyXG4gICAgYW5ndWxhci5mb3JFYWNoIGRhdGEudmVydGljZXMsICh2ZXJ0ZXgsIGkpIC0+XHJcbiAgICAgIHZlcnRleC50eXBlID0gJ3JlZ3VsYXInXHJcblxyXG4gICAgZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcclxuICAgICAgbmFtZTogJ1NjaGVkdWxlZCdcclxuICAgICAgJ3N0YXJ0LXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXVxyXG4gICAgICAnZW5kLXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSArIDFcclxuICAgICAgdHlwZTogJ3NjaGVkdWxlZCdcclxuICAgIH0pXHJcblxyXG4gIEBsaXN0Sm9icyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL292ZXJ2aWV3XCJcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cclxuICAgICAgIyByZXNldCBqb2IgZmllbGRzXHJcbiAgICAgIGpvYnMuZmluaXNoZWQgPSBbXVxyXG4gICAgICBqb2JzLnJ1bm5pbmcgPSBbXVxyXG5cclxuICAgICAgIyBncm91cCB0aGUgcmVjZWl2ZWQgbGlzdCBvZiBqb2JzIGludG8gcnVubmluZyBhbmQgZmluaXNoZWQgam9ic1xyXG4gICAgICBfKGRhdGEuam9icykuZ3JvdXBCeShcclxuICAgICAgICAoeCkgLT5cclxuICAgICAgICAgIHN3aXRjaCB4LnN0YXRlLnRvTG93ZXJDYXNlKClcclxuICAgICAgICAgICAgd2hlbiAnZmluaXNoZWQnIHRoZW4gJ2ZpbmlzaGVkJ1xyXG4gICAgICAgICAgICB3aGVuICdmYWlsZWQnIHRoZW4gJ2ZpbmlzaGVkJ1xyXG4gICAgICAgICAgICB3aGVuICdjYW5jZWxlZCcgdGhlbiAnZmluaXNoZWQnXHJcbiAgICAgICAgICAgIGVsc2UgJ3J1bm5pbmcnKVxyXG4gICAgICAuZm9yRWFjaCgodmFsdWUsIGtleSkgPT5cclxuICAgICAgICBzd2l0Y2gga2V5XHJcbiAgICAgICAgICB3aGVuICdmaW5pc2hlZCcgdGhlbiBqb2JzLmZpbmlzaGVkID0gQHNldEVuZFRpbWVzKHZhbHVlKVxyXG4gICAgICAgICAgd2hlbiAncnVubmluZycgdGhlbiBqb2JzLnJ1bm5pbmcgPSBAc2V0RW5kVGltZXModmFsdWUpKVxyXG4gICAgICAudmFsdWUoKTsgIyBtYXRlcmlhbGl6ZSB0aGUgY2hhaW5cclxuXHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoam9icylcclxuICAgICAgbm90aWZ5T2JzZXJ2ZXJzKClcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBnZXRKb2JzID0gKHR5cGUpIC0+XHJcbiAgICBqb2JzW3R5cGVdXHJcblxyXG4gIEBnZXRBbGxKb2JzID0gLT5cclxuICAgIGpvYnNcclxuXHJcbiAgQGxvYWRKb2IgPSAoam9iaWQpIC0+XHJcbiAgICBjdXJyZW50Sm9iID0gbnVsbFxyXG4gICAgZGVmZXJyZWRzLmpvYiA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWRcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgPT5cclxuICAgICAgQHNldEVuZFRpbWVzKGRhdGEudmVydGljZXMpXHJcbiAgICAgIEBwcm9jZXNzVmVydGljZXMoZGF0YSlcclxuXHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL2NvbmZpZ1wiXHJcbiAgICAgIC5zdWNjZXNzIChqb2JDb25maWcpIC0+XHJcbiAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZylcclxuXHJcbiAgICAgICAgY3VycmVudEpvYiA9IGRhdGFcclxuXHJcbiAgICAgICAgZGVmZXJyZWRzLmpvYi5yZXNvbHZlKGN1cnJlbnRKb2IpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlXHJcblxyXG4gIEBnZXROb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIHNlZWtOb2RlID0gKG5vZGVpZCwgZGF0YSkgLT5cclxuICAgICAgZm9yIG5vZGUgaW4gZGF0YVxyXG4gICAgICAgIHJldHVybiBub2RlIGlmIG5vZGUuaWQgaXMgbm9kZWlkXHJcbiAgICAgICAgc3ViID0gc2Vla05vZGUobm9kZWlkLCBub2RlLnN0ZXBfZnVuY3Rpb24pIGlmIG5vZGUuc3RlcF9mdW5jdGlvblxyXG4gICAgICAgIHJldHVybiBzdWIgaWYgc3ViXHJcblxyXG4gICAgICBudWxsXHJcblxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XHJcbiAgICAgIGZvdW5kTm9kZSA9IHNlZWtOb2RlKG5vZGVpZCwgY3VycmVudEpvYi5wbGFuLm5vZGVzKVxyXG5cclxuICAgICAgZm91bmROb2RlLnZlcnRleCA9IEBzZWVrVmVydGV4KG5vZGVpZClcclxuXHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZm91bmROb2RlKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQHNlZWtWZXJ0ZXggPSAobm9kZWlkKSAtPlxyXG4gICAgZm9yIHZlcnRleCBpbiBjdXJyZW50Sm9iLnZlcnRpY2VzXHJcbiAgICAgIHJldHVybiB2ZXJ0ZXggaWYgdmVydGV4LmlkIGlzIG5vZGVpZFxyXG5cclxuICAgIHJldHVybiBudWxsXHJcblxyXG4gIEBnZXRWZXJ0ZXggPSAodmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgdmVydGV4ID0gQHNlZWtWZXJ0ZXgodmVydGV4aWQpXHJcblxyXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvc3VidGFza3RpbWVzXCJcclxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpID0+XHJcbiAgICAgICAgIyBUT0RPOiBjaGFuZ2UgdG8gc3VidGFza3RpbWVzXHJcbiAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrc1xyXG5cclxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHZlcnRleClcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBnZXRTdWJ0YXNrcyA9ICh2ZXJ0ZXhpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICAjIHZlcnRleCA9IEBzZWVrVmVydGV4KHZlcnRleGlkKVxyXG5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZFxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cclxuICAgICAgICBzdWJ0YXNrcyA9IGRhdGEuc3VidGFza3NcclxuXHJcbiAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShzdWJ0YXNrcylcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBnZXRUYXNrTWFuYWdlcnMgPSAodmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcclxuXHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi90YXNrbWFuYWdlcnNcIlxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cclxuICAgICAgICB0YXNrbWFuYWdlcnMgPSBkYXRhLnRhc2ttYW5hZ2Vyc1xyXG5cclxuICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHRhc2ttYW5hZ2VycylcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBnZXRBY2N1bXVsYXRvcnMgPSAodmVydGV4aWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgIyB2ZXJ0ZXggPSBAc2Vla1ZlcnRleCh2ZXJ0ZXhpZClcclxuICAgICAgY29uc29sZS5sb2coY3VycmVudEpvYi5qaWQpXHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9hY2N1bXVsYXRvcnNcIlxyXG4gICAgICAuc3VjY2VzcyAoZGF0YSkgLT5cclxuICAgICAgICBhY2N1bXVsYXRvcnMgPSBkYXRhWyd1c2VyLWFjY3VtdWxhdG9ycyddXHJcblxyXG4gICAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrcy9hY2N1bXVsYXRvcnNcIlxyXG4gICAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgICAgc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3NcclxuXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKHsgbWFpbjogYWNjdW11bGF0b3JzLCBzdWJ0YXNrczogc3VidGFza0FjY3VtdWxhdG9ycyB9KVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgIyBDaGVja3BvaW50IGNvbmZpZ1xyXG4gIEBnZXRDaGVja3BvaW50Q29uZmlnID0gIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuICAgICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvY2hlY2twb2ludHMvY29uZmlnXCJcclxuICAgICAgLnN1Y2Nlc3MgKGRhdGEpIC0+XHJcbiAgICAgICAgaWYgKGFuZ3VsYXIuZXF1YWxzKHt9LCBkYXRhKSlcclxuICAgICAgICAgIGRlZmVycmVkLnJlc29sdmUobnVsbClcclxuICAgICAgICBlbHNlXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICAjIEdlbmVyYWwgY2hlY2twb2ludCBzdGF0cyBsaWtlIGNvdW50cywgaGlzdG9yeSwgZXRjLlxyXG4gIEBnZXRDaGVja3BvaW50U3RhdHMgPSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzXCJcclxuICAgICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSA9PlxyXG4gICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKG51bGwpXHJcbiAgICAgICAgZWxzZVxyXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgIyBEZXRhaWxlZCBjaGVja3BvaW50IHN0YXRzIGZvciBhIHNpbmdsZSBjaGVja3BvaW50XHJcbiAgQGdldENoZWNrcG9pbnREZXRhaWxzID0gKGNoZWNrcG9pbnRpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuIChkYXRhKSA9PlxyXG4gICAgICAkaHR0cC5nZXQgZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi9jaGVja3BvaW50cy9kZXRhaWxzL1wiICsgY2hlY2twb2ludGlkXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgICMgSWYgbm8gZGF0YSBhdmFpbGFibGUsIHdlIGFyZSBkb25lLlxyXG4gICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKG51bGwpXHJcbiAgICAgICAgZWxzZVxyXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgIyBEZXRhaWxlZCBzdWJ0YXNrIHN0YXRzIGZvciBhIHNpbmdsZSBjaGVja3BvaW50XHJcbiAgQGdldENoZWNrcG9pbnRTdWJ0YXNrRGV0YWlscyA9IChjaGVja3BvaW50aWQsIHZlcnRleGlkKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4gKGRhdGEpID0+XHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzL2RldGFpbHMvXCIgKyBjaGVja3BvaW50aWQgKyBcIi9zdWJ0YXNrcy9cIiArIHZlcnRleGlkXHJcbiAgICAgIC5zdWNjZXNzIChkYXRhKSAtPlxyXG4gICAgICAgICMgSWYgbm8gZGF0YSBhdmFpbGFibGUsIHdlIGFyZSBkb25lLlxyXG4gICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpXHJcbiAgICAgICAgICBkZWZlcnJlZC5yZXNvbHZlKG51bGwpXHJcbiAgICAgICAgZWxzZVxyXG4gICAgICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgIyBPcGVyYXRvci1sZXZlbCBiYWNrIHByZXNzdXJlIHN0YXRzXHJcbiAgQGdldE9wZXJhdG9yQmFja1ByZXNzdXJlID0gKHZlcnRleGlkKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2JhY2twcmVzc3VyZVwiXHJcbiAgICAuc3VjY2VzcyAoZGF0YSkgPT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQHRyYW5zbGF0ZUJhY2tQcmVzc3VyZUxhYmVsU3RhdGUgPSAoc3RhdGUpIC0+XHJcbiAgICBzd2l0Y2ggc3RhdGUudG9Mb3dlckNhc2UoKVxyXG4gICAgICB3aGVuICdpbi1wcm9ncmVzcycgdGhlbiAnZGFuZ2VyJ1xyXG4gICAgICB3aGVuICdvaycgdGhlbiAnc3VjY2VzcydcclxuICAgICAgd2hlbiAnbG93JyB0aGVuICd3YXJuaW5nJ1xyXG4gICAgICB3aGVuICdoaWdoJyB0aGVuICdkYW5nZXInXHJcbiAgICAgIGVsc2UgJ2RlZmF1bHQnXHJcblxyXG4gIEBsb2FkRXhjZXB0aW9ucyA9IC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbiAoZGF0YSkgPT5cclxuXHJcbiAgICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2V4Y2VwdGlvbnNcIlxyXG4gICAgICAuc3VjY2VzcyAoZXhjZXB0aW9ucykgLT5cclxuICAgICAgICBjdXJyZW50Sm9iLmV4Y2VwdGlvbnMgPSBleGNlcHRpb25zXHJcblxyXG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoZXhjZXB0aW9ucylcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBjYW5jZWxKb2IgPSAoam9iaWQpIC0+XHJcbiAgICAjIHVzZXMgdGhlIG5vbiBSRVNULWNvbXBsaWFudCBHRVQgeWFybi1jYW5jZWwgaGFuZGxlciB3aGljaCBpcyBhdmFpbGFibGUgaW4gYWRkaXRpb24gdG8gdGhlXHJcbiAgICAjIHByb3BlciAkaHR0cC5wYXRjaCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiP21vZGU9Y2FuY2VsXCJcclxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3lhcm4tY2FuY2VsXCJcclxuXHJcbiAgQHN0b3BKb2IgPSAoam9iaWQpIC0+XHJcbiAgICAjIHVzZXMgdGhlIG5vbiBSRVNULWNvbXBsaWFudCBHRVQgeWFybi1jYW5jZWwgaGFuZGxlciB3aGljaCBpcyBhdmFpbGFibGUgaW4gYWRkaXRpb24gdG8gdGhlXHJcbiAgICAjIHByb3BlciAkaHR0cC5wYXRjaCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiP21vZGU9c3RvcFwiXHJcbiAgICAkaHR0cC5nZXQgXCJqb2JzL1wiICsgam9iaWQgKyBcIi95YXJuLXN0b3BcIlxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYnNTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkbG9nLCBhbU1vbWVudCwgJHEsICR0aW1lb3V0KSB7XG4gIHZhciBjdXJyZW50Sm9iLCBjdXJyZW50UGxhbiwgZGVmZXJyZWRzLCBqb2JPYnNlcnZlcnMsIGpvYnMsIG5vdGlmeU9ic2VydmVycztcbiAgY3VycmVudEpvYiA9IG51bGw7XG4gIGN1cnJlbnRQbGFuID0gbnVsbDtcbiAgZGVmZXJyZWRzID0ge307XG4gIGpvYnMgPSB7XG4gICAgcnVubmluZzogW10sXG4gICAgZmluaXNoZWQ6IFtdLFxuICAgIGNhbmNlbGxlZDogW10sXG4gICAgZmFpbGVkOiBbXVxuICB9O1xuICBqb2JPYnNlcnZlcnMgPSBbXTtcbiAgbm90aWZ5T2JzZXJ2ZXJzID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaChqb2JPYnNlcnZlcnMsIGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgICByZXR1cm4gY2FsbGJhY2soKTtcbiAgICB9KTtcbiAgfTtcbiAgdGhpcy5yZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oY2FsbGJhY2spIHtcbiAgICByZXR1cm4gam9iT2JzZXJ2ZXJzLnB1c2goY2FsbGJhY2spO1xuICB9O1xuICB0aGlzLnVuUmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGNhbGxiYWNrKSB7XG4gICAgdmFyIGluZGV4O1xuICAgIGluZGV4ID0gam9iT2JzZXJ2ZXJzLmluZGV4T2YoY2FsbGJhY2spO1xuICAgIHJldHVybiBqb2JPYnNlcnZlcnMuc3BsaWNlKGluZGV4LCAxKTtcbiAgfTtcbiAgdGhpcy5zdGF0ZUxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gWydTQ0hFRFVMRUQnLCAnREVQTE9ZSU5HJywgJ1JVTk5JTkcnLCAnRklOSVNIRUQnLCAnRkFJTEVEJywgJ0NBTkNFTElORycsICdDQU5DRUxFRCddO1xuICB9O1xuICB0aGlzLnRyYW5zbGF0ZUxhYmVsU3RhdGUgPSBmdW5jdGlvbihzdGF0ZSkge1xuICAgIHN3aXRjaCAoc3RhdGUudG9Mb3dlckNhc2UoKSkge1xuICAgICAgY2FzZSAnZmluaXNoZWQnOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnZmFpbGVkJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnc2NoZWR1bGVkJzpcbiAgICAgICAgcmV0dXJuICdkZWZhdWx0JztcbiAgICAgIGNhc2UgJ2RlcGxveWluZyc6XG4gICAgICAgIHJldHVybiAnaW5mbyc7XG4gICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgcmV0dXJuICdwcmltYXJ5JztcbiAgICAgIGNhc2UgJ2NhbmNlbGluZyc6XG4gICAgICAgIHJldHVybiAnd2FybmluZyc7XG4gICAgICBjYXNlICdwZW5kaW5nJzpcbiAgICAgICAgcmV0dXJuICdpbmZvJztcbiAgICAgIGNhc2UgJ3RvdGFsJzpcbiAgICAgICAgcmV0dXJuICdibGFjayc7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5zZXRFbmRUaW1lcyA9IGZ1bmN0aW9uKGxpc3QpIHtcbiAgICByZXR1cm4gYW5ndWxhci5mb3JFYWNoKGxpc3QsIGZ1bmN0aW9uKGl0ZW0sIGpvYktleSkge1xuICAgICAgaWYgKCEoaXRlbVsnZW5kLXRpbWUnXSA+IC0xKSkge1xuICAgICAgICByZXR1cm4gaXRlbVsnZW5kLXRpbWUnXSA9IGl0ZW1bJ3N0YXJ0LXRpbWUnXSArIGl0ZW1bJ2R1cmF0aW9uJ107XG4gICAgICB9XG4gICAgfSk7XG4gIH07XG4gIHRoaXMucHJvY2Vzc1ZlcnRpY2VzID0gZnVuY3Rpb24oZGF0YSkge1xuICAgIGFuZ3VsYXIuZm9yRWFjaChkYXRhLnZlcnRpY2VzLCBmdW5jdGlvbih2ZXJ0ZXgsIGkpIHtcbiAgICAgIHJldHVybiB2ZXJ0ZXgudHlwZSA9ICdyZWd1bGFyJztcbiAgICB9KTtcbiAgICByZXR1cm4gZGF0YS52ZXJ0aWNlcy51bnNoaWZ0KHtcbiAgICAgIG5hbWU6ICdTY2hlZHVsZWQnLFxuICAgICAgJ3N0YXJ0LXRpbWUnOiBkYXRhLnRpbWVzdGFtcHNbJ0NSRUFURUQnXSxcbiAgICAgICdlbmQtdGltZSc6IGRhdGEudGltZXN0YW1wc1snQ1JFQVRFRCddICsgMSxcbiAgICAgIHR5cGU6ICdzY2hlZHVsZWQnXG4gICAgfSk7XG4gIH07XG4gIHRoaXMubGlzdEpvYnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvb3ZlcnZpZXdcIikuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBqb2JzLmZpbmlzaGVkID0gW107XG4gICAgICAgIGpvYnMucnVubmluZyA9IFtdO1xuICAgICAgICBfKGRhdGEuam9icykuZ3JvdXBCeShmdW5jdGlvbih4KSB7XG4gICAgICAgICAgc3dpdGNoICh4LnN0YXRlLnRvTG93ZXJDYXNlKCkpIHtcbiAgICAgICAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuICdmaW5pc2hlZCc7XG4gICAgICAgICAgICBjYXNlICdmYWlsZWQnOlxuICAgICAgICAgICAgICByZXR1cm4gJ2ZpbmlzaGVkJztcbiAgICAgICAgICAgIGNhc2UgJ2NhbmNlbGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuICdmaW5pc2hlZCc7XG4gICAgICAgICAgICBkZWZhdWx0OlxuICAgICAgICAgICAgICByZXR1cm4gJ3J1bm5pbmcnO1xuICAgICAgICAgIH1cbiAgICAgICAgfSkuZm9yRWFjaChmdW5jdGlvbih2YWx1ZSwga2V5KSB7XG4gICAgICAgICAgc3dpdGNoIChrZXkpIHtcbiAgICAgICAgICAgIGNhc2UgJ2ZpbmlzaGVkJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMuZmluaXNoZWQgPSBfdGhpcy5zZXRFbmRUaW1lcyh2YWx1ZSk7XG4gICAgICAgICAgICBjYXNlICdydW5uaW5nJzpcbiAgICAgICAgICAgICAgcmV0dXJuIGpvYnMucnVubmluZyA9IF90aGlzLnNldEVuZFRpbWVzKHZhbHVlKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pLnZhbHVlKCk7XG4gICAgICAgIGRlZmVycmVkLnJlc29sdmUoam9icyk7XG4gICAgICAgIHJldHVybiBub3RpZnlPYnNlcnZlcnMoKTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldEpvYnMgPSBmdW5jdGlvbih0eXBlKSB7XG4gICAgcmV0dXJuIGpvYnNbdHlwZV07XG4gIH07XG4gIHRoaXMuZ2V0QWxsSm9icyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBqb2JzO1xuICB9O1xuICB0aGlzLmxvYWRKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIGN1cnJlbnRKb2IgPSBudWxsO1xuICAgIGRlZmVycmVkcy5qb2IgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCkuc3VjY2VzcygoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICBfdGhpcy5zZXRFbmRUaW1lcyhkYXRhLnZlcnRpY2VzKTtcbiAgICAgICAgX3RoaXMucHJvY2Vzc1ZlcnRpY2VzKGRhdGEpO1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvY29uZmlnXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oam9iQ29uZmlnKSB7XG4gICAgICAgICAgZGF0YSA9IGFuZ3VsYXIuZXh0ZW5kKGRhdGEsIGpvYkNvbmZpZyk7XG4gICAgICAgICAgY3VycmVudEpvYiA9IGRhdGE7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkcy5qb2IucmVzb2x2ZShjdXJyZW50Sm9iKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWRzLmpvYi5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldE5vZGUgPSBmdW5jdGlvbihub2RlaWQpIHtcbiAgICB2YXIgZGVmZXJyZWQsIHNlZWtOb2RlO1xuICAgIHNlZWtOb2RlID0gZnVuY3Rpb24obm9kZWlkLCBkYXRhKSB7XG4gICAgICB2YXIgaiwgbGVuLCBub2RlLCBzdWI7XG4gICAgICBmb3IgKGogPSAwLCBsZW4gPSBkYXRhLmxlbmd0aDsgaiA8IGxlbjsgaisrKSB7XG4gICAgICAgIG5vZGUgPSBkYXRhW2pdO1xuICAgICAgICBpZiAobm9kZS5pZCA9PT0gbm9kZWlkKSB7XG4gICAgICAgICAgcmV0dXJuIG5vZGU7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKG5vZGUuc3RlcF9mdW5jdGlvbikge1xuICAgICAgICAgIHN1YiA9IHNlZWtOb2RlKG5vZGVpZCwgbm9kZS5zdGVwX2Z1bmN0aW9uKTtcbiAgICAgICAgfVxuICAgICAgICBpZiAoc3ViKSB7XG4gICAgICAgICAgcmV0dXJuIHN1YjtcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIG51bGw7XG4gICAgfTtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgZm91bmROb2RlO1xuICAgICAgICBmb3VuZE5vZGUgPSBzZWVrTm9kZShub2RlaWQsIGN1cnJlbnRKb2IucGxhbi5ub2Rlcyk7XG4gICAgICAgIGZvdW5kTm9kZS52ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KG5vZGVpZCk7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGZvdW5kTm9kZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZWVrVmVydGV4ID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgdmFyIGosIGxlbiwgcmVmLCB2ZXJ0ZXg7XG4gICAgcmVmID0gY3VycmVudEpvYi52ZXJ0aWNlcztcbiAgICBmb3IgKGogPSAwLCBsZW4gPSByZWYubGVuZ3RoOyBqIDwgbGVuOyBqKyspIHtcbiAgICAgIHZlcnRleCA9IHJlZltqXTtcbiAgICAgIGlmICh2ZXJ0ZXguaWQgPT09IG5vZGVpZCkge1xuICAgICAgICByZXR1cm4gdmVydGV4O1xuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gbnVsbDtcbiAgfTtcbiAgdGhpcy5nZXRWZXJ0ZXggPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgdmVydGV4O1xuICAgICAgICB2ZXJ0ZXggPSBfdGhpcy5zZWVrVmVydGV4KHZlcnRleGlkKTtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQgKyBcIi9zdWJ0YXNrdGltZXNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgICAgdmVydGV4LnN1YnRhc2tzID0gZGF0YS5zdWJ0YXNrcztcbiAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh2ZXJ0ZXgpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldFN1YnRhc2tzID0gZnVuY3Rpb24odmVydGV4aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL3ZlcnRpY2VzL1wiICsgdmVydGV4aWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBzdWJ0YXNrcztcbiAgICAgICAgICBzdWJ0YXNrcyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoc3VidGFza3MpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldFRhc2tNYW5hZ2VycyA9IGZ1bmN0aW9uKHZlcnRleGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICBkZWZlcnJlZHMuam9iLnByb21pc2UudGhlbigoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvdGFza21hbmFnZXJzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciB0YXNrbWFuYWdlcnM7XG4gICAgICAgICAgdGFza21hbmFnZXJzID0gZGF0YS50YXNrbWFuYWdlcnM7XG4gICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUodGFza21hbmFnZXJzKTtcbiAgICAgICAgfSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRBY2N1bXVsYXRvcnMgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICBjb25zb2xlLmxvZyhjdXJyZW50Sm9iLmppZCk7XG4gICAgICAgIHJldHVybiAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgY3VycmVudEpvYi5qaWQgKyBcIi92ZXJ0aWNlcy9cIiArIHZlcnRleGlkICsgXCIvYWNjdW11bGF0b3JzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIHZhciBhY2N1bXVsYXRvcnM7XG4gICAgICAgICAgYWNjdW11bGF0b3JzID0gZGF0YVsndXNlci1hY2N1bXVsYXRvcnMnXTtcbiAgICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL3N1YnRhc2tzL2FjY3VtdWxhdG9yc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICAgIHZhciBzdWJ0YXNrQWNjdW11bGF0b3JzO1xuICAgICAgICAgICAgc3VidGFza0FjY3VtdWxhdG9ycyA9IGRhdGEuc3VidGFza3M7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZSh7XG4gICAgICAgICAgICAgIG1haW46IGFjY3VtdWxhdG9ycyxcbiAgICAgICAgICAgICAgc3VidGFza3M6IHN1YnRhc2tBY2N1bXVsYXRvcnNcbiAgICAgICAgICAgIH0pO1xuICAgICAgICAgIH0pO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldENoZWNrcG9pbnRDb25maWcgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzL2NvbmZpZ1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgICBpZiAoYW5ndWxhci5lcXVhbHMoe30sIGRhdGEpKSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShudWxsKTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICAgICAgfVxuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmdldENoZWNrcG9pbnRTdGF0cyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvY2hlY2twb2ludHNcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKG51bGwpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0Q2hlY2twb2ludERldGFpbHMgPSBmdW5jdGlvbihjaGVja3BvaW50aWQpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGRlZmVycmVkcy5qb2IucHJvbWlzZS50aGVuKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBjdXJyZW50Sm9iLmppZCArIFwiL2NoZWNrcG9pbnRzL2RldGFpbHMvXCIgKyBjaGVja3BvaW50aWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKG51bGwpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0Q2hlY2twb2ludFN1YnRhc2tEZXRhaWxzID0gZnVuY3Rpb24oY2hlY2twb2ludGlkLCB2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvY2hlY2twb2ludHMvZGV0YWlscy9cIiArIGNoZWNrcG9pbnRpZCArIFwiL3N1YnRhc2tzL1wiICsgdmVydGV4aWQpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICAgIGlmIChhbmd1bGFyLmVxdWFscyh7fSwgZGF0YSkpIHtcbiAgICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKG51bGwpO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMuZ2V0T3BlcmF0b3JCYWNrUHJlc3N1cmUgPSBmdW5jdGlvbih2ZXJ0ZXhpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvdmVydGljZXMvXCIgKyB2ZXJ0ZXhpZCArIFwiL2JhY2twcmVzc3VyZVwiKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy50cmFuc2xhdGVCYWNrUHJlc3N1cmVMYWJlbFN0YXRlID0gZnVuY3Rpb24oc3RhdGUpIHtcbiAgICBzd2l0Y2ggKHN0YXRlLnRvTG93ZXJDYXNlKCkpIHtcbiAgICAgIGNhc2UgJ2luLXByb2dyZXNzJzpcbiAgICAgICAgcmV0dXJuICdkYW5nZXInO1xuICAgICAgY2FzZSAnb2snOlxuICAgICAgICByZXR1cm4gJ3N1Y2Nlc3MnO1xuICAgICAgY2FzZSAnbG93JzpcbiAgICAgICAgcmV0dXJuICd3YXJuaW5nJztcbiAgICAgIGNhc2UgJ2hpZ2gnOlxuICAgICAgICByZXR1cm4gJ2Rhbmdlcic7XG4gICAgICBkZWZhdWx0OlxuICAgICAgICByZXR1cm4gJ2RlZmF1bHQnO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5sb2FkRXhjZXB0aW9ucyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgZGVmZXJyZWRzLmpvYi5wcm9taXNlLnRoZW4oKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICByZXR1cm4gJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGN1cnJlbnRKb2IuamlkICsgXCIvZXhjZXB0aW9uc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGV4Y2VwdGlvbnMpIHtcbiAgICAgICAgICBjdXJyZW50Sm9iLmV4Y2VwdGlvbnMgPSBleGNlcHRpb25zO1xuICAgICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGV4Y2VwdGlvbnMpO1xuICAgICAgICB9KTtcbiAgICAgIH07XG4gICAgfSkodGhpcykpO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmNhbmNlbEpvYiA9IGZ1bmN0aW9uKGpvYmlkKSB7XG4gICAgcmV0dXJuICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3lhcm4tY2FuY2VsXCIpO1xuICB9O1xuICB0aGlzLnN0b3BKb2IgPSBmdW5jdGlvbihqb2JpZCkge1xuICAgIHJldHVybiAkaHR0cC5nZXQoXCJqb2JzL1wiICsgam9iaWQgKyBcIi95YXJuLXN0b3BcIik7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbiMgLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLVxyXG5cclxuLmRpcmVjdGl2ZSAnbWV0cmljc0dyYXBoJywgLT5cclxuICB0ZW1wbGF0ZTogJzxkaXYgY2xhc3M9XCJwYW5lbCBwYW5lbC1kZWZhdWx0IHBhbmVsLW1ldHJpY1wiPlxyXG4gICAgICAgICAgICAgICA8ZGl2IGNsYXNzPVwicGFuZWwtaGVhZGluZ1wiPlxyXG4gICAgICAgICAgICAgICAgIDxzcGFuIGNsYXNzPVwibWV0cmljLXRpdGxlXCI+e3ttZXRyaWMuaWR9fTwvc3Bhbj5cclxuICAgICAgICAgICAgICAgICA8ZGl2IGNsYXNzPVwiYnV0dG9uc1wiPlxyXG4gICAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz1cImJ0bi1ncm91cFwiPlxyXG4gICAgICAgICAgICAgICAgICAgICA8YnV0dG9uIHR5cGU9XCJidXR0b25cIiBuZy1jbGFzcz1cIltidG5DbGFzc2VzLCB7YWN0aXZlOiBtZXRyaWMuc2l6ZSAhPSBcXCdiaWdcXCd9XVwiIG5nLWNsaWNrPVwic2V0U2l6ZShcXCdzbWFsbFxcJylcIj5TbWFsbDwvYnV0dG9uPlxyXG4gICAgICAgICAgICAgICAgICAgICA8YnV0dG9uIHR5cGU9XCJidXR0b25cIiBuZy1jbGFzcz1cIltidG5DbGFzc2VzLCB7YWN0aXZlOiBtZXRyaWMuc2l6ZSA9PSBcXCdiaWdcXCd9XVwiIG5nLWNsaWNrPVwic2V0U2l6ZShcXCdiaWdcXCcpXCI+QmlnPC9idXR0b24+XHJcbiAgICAgICAgICAgICAgICAgICA8L2Rpdj5cclxuICAgICAgICAgICAgICAgICAgIDxhIHRpdGxlPVwiUmVtb3ZlXCIgY2xhc3M9XCJidG4gYnRuLWRlZmF1bHQgYnRuLXhzIHJlbW92ZVwiIG5nLWNsaWNrPVwicmVtb3ZlTWV0cmljKClcIj48aSBjbGFzcz1cImZhIGZhLWNsb3NlXCIgLz48L2E+XHJcbiAgICAgICAgICAgICAgICAgPC9kaXY+XHJcbiAgICAgICAgICAgICAgIDwvZGl2PlxyXG4gICAgICAgICAgICAgICA8ZGl2IGNsYXNzPVwicGFuZWwtYm9keVwiPlxyXG4gICAgICAgICAgICAgICAgICA8c3ZnIG5nLWlmPVwibWV0cmljLnZpZXcgPT0gXFwnY2hhcnRcXCdcIi8+XHJcbiAgICAgICAgICAgICAgICAgIDxkaXYgbmctaWY9XCJtZXRyaWMudmlldyAhPSBcXCdjaGFydFxcJ1wiPlxyXG4gICAgICAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz1cIm1ldHJpYy1udW1lcmljXCIgdGl0bGU9XCJ7e3ZhbHVlIHwgaHVtYW5pemVDaGFydE51bWVyaWNUaXRsZTptZXRyaWN9fVwiPnt7dmFsdWUgfCBodW1hbml6ZUNoYXJ0TnVtZXJpYzptZXRyaWN9fTwvZGl2PlxyXG4gICAgICAgICAgICAgICAgICA8L2Rpdj5cclxuICAgICAgICAgICAgICAgPC9kaXY+XHJcbiAgICAgICAgICAgICAgIDxkaXYgY2xhc3M9XCJidXR0b25zXCI+XHJcbiAgICAgICAgICAgICAgICAgPGRpdiBjbGFzcz1cImJ0bi1ncm91cFwiPlxyXG4gICAgICAgICAgICAgICAgICAgPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgbmctY2xhc3M9XCJbYnRuQ2xhc3Nlcywge2FjdGl2ZTogbWV0cmljLnZpZXcgPT0gXFwnY2hhcnRcXCd9XVwiIG5nLWNsaWNrPVwic2V0VmlldyhcXCdjaGFydFxcJylcIj5DaGFydDwvYnV0dG9uPlxyXG4gICAgICAgICAgICAgICAgICAgPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgbmctY2xhc3M9XCJbYnRuQ2xhc3Nlcywge2FjdGl2ZTogbWV0cmljLnZpZXcgIT0gXFwnY2hhcnRcXCd9XVwiIG5nLWNsaWNrPVwic2V0VmlldyhcXCdudW1lcmljXFwnKVwiPk51bWVyaWM8L2J1dHRvbj5cclxuICAgICAgICAgICAgICAgICA8L2Rpdj5cclxuICAgICAgICAgICAgIDwvZGl2PidcclxuICByZXBsYWNlOiB0cnVlXHJcbiAgc2NvcGU6XHJcbiAgICBtZXRyaWM6IFwiPVwiXHJcbiAgICB3aW5kb3c6IFwiPVwiXHJcbiAgICByZW1vdmVNZXRyaWM6IFwiJlwiXHJcbiAgICBzZXRNZXRyaWNTaXplOiBcIj1cIlxyXG4gICAgc2V0TWV0cmljVmlldzogXCI9XCJcclxuICAgIGdldFZhbHVlczogXCImXCJcclxuXHJcbiAgbGluazogKHNjb3BlLCBlbGVtZW50LCBhdHRycykgLT5cclxuICAgIHNjb3BlLmJ0bkNsYXNzZXMgPSBbJ2J0bicsICdidG4tZGVmYXVsdCcsICdidG4teHMnXVxyXG5cclxuICAgIHNjb3BlLnZhbHVlID0gbnVsbFxyXG4gICAgc2NvcGUuZGF0YSA9IFt7XHJcbiAgICAgIHZhbHVlczogc2NvcGUuZ2V0VmFsdWVzKClcclxuICAgIH1dXHJcblxyXG4gICAgc2NvcGUub3B0aW9ucyA9IHtcclxuICAgICAgeDogKGQsIGkpIC0+XHJcbiAgICAgICAgZC54XHJcbiAgICAgIHk6IChkLCBpKSAtPlxyXG4gICAgICAgIGQueVxyXG5cclxuICAgICAgeFRpY2tGb3JtYXQ6IChkKSAtPlxyXG4gICAgICAgIGQzLnRpbWUuZm9ybWF0KCclSDolTTolUycpKG5ldyBEYXRlKGQpKVxyXG5cclxuICAgICAgeVRpY2tGb3JtYXQ6IChkKSAtPlxyXG4gICAgICAgIGZvdW5kID0gZmFsc2VcclxuICAgICAgICBwb3cgPSAwXHJcbiAgICAgICAgc3RlcCA9IDFcclxuICAgICAgICBhYnNEID0gTWF0aC5hYnMoZClcclxuXHJcbiAgICAgICAgd2hpbGUgIWZvdW5kICYmIHBvdyA8IDUwXHJcbiAgICAgICAgICBpZiBNYXRoLnBvdygxMCwgcG93KSA8PSBhYnNEICYmIGFic0QgPCBNYXRoLnBvdygxMCwgcG93ICsgc3RlcClcclxuICAgICAgICAgICAgZm91bmQgPSB0cnVlXHJcbiAgICAgICAgICBlbHNlXHJcbiAgICAgICAgICAgIHBvdyArPSBzdGVwXHJcblxyXG4gICAgICAgIGlmIGZvdW5kICYmIHBvdyA+IDZcclxuICAgICAgICAgIFwiI3tkIC8gTWF0aC5wb3coMTAsIHBvdyl9RSN7cG93fVwiXHJcbiAgICAgICAgZWxzZVxyXG4gICAgICAgICAgXCIje2R9XCJcclxuICAgIH1cclxuXHJcbiAgICBzY29wZS5zaG93Q2hhcnQgPSAtPlxyXG4gICAgICBkMy5zZWxlY3QoZWxlbWVudC5maW5kKFwic3ZnXCIpWzBdKVxyXG4gICAgICAuZGF0dW0oc2NvcGUuZGF0YSlcclxuICAgICAgLnRyYW5zaXRpb24oKS5kdXJhdGlvbigyNTApXHJcbiAgICAgIC5jYWxsKHNjb3BlLmNoYXJ0KVxyXG5cclxuICAgIHNjb3BlLmNoYXJ0ID0gbnYubW9kZWxzLmxpbmVDaGFydCgpXHJcbiAgICAgIC5vcHRpb25zKHNjb3BlLm9wdGlvbnMpXHJcbiAgICAgIC5zaG93TGVnZW5kKGZhbHNlKVxyXG4gICAgICAubWFyZ2luKHtcclxuICAgICAgICB0b3A6IDE1XHJcbiAgICAgICAgbGVmdDogNjBcclxuICAgICAgICBib3R0b206IDMwXHJcbiAgICAgICAgcmlnaHQ6IDMwXHJcbiAgICAgIH0pXHJcblxyXG4gICAgc2NvcGUuY2hhcnQueUF4aXMuc2hvd01heE1pbihmYWxzZSlcclxuICAgIHNjb3BlLmNoYXJ0LnRvb2x0aXAuaGlkZURlbGF5KDApXHJcbiAgICBzY29wZS5jaGFydC50b29sdGlwLmNvbnRlbnRHZW5lcmF0b3IoKG9iaikgLT5cclxuICAgICAgXCI8cD4je2QzLnRpbWUuZm9ybWF0KCclSDolTTolUycpKG5ldyBEYXRlKG9iai5wb2ludC54KSl9IHwgI3tvYmoucG9pbnQueX08L3A+XCJcclxuICAgIClcclxuXHJcbiAgICBudi51dGlscy53aW5kb3dSZXNpemUoc2NvcGUuY2hhcnQudXBkYXRlKTtcclxuXHJcbiAgICBzY29wZS5zZXRTaXplID0gKHNpemUpIC0+XHJcbiAgICAgIHNjb3BlLnNldE1ldHJpY1NpemUoc2NvcGUubWV0cmljLCBzaXplKVxyXG5cclxuICAgIHNjb3BlLnNldFZpZXcgPSAodmlldykgLT5cclxuICAgICAgc2NvcGUuc2V0TWV0cmljVmlldyhzY29wZS5tZXRyaWMsIHZpZXcpXHJcbiAgICAgIHNjb3BlLnNob3dDaGFydCgpIGlmIHZpZXcgPT0gJ2NoYXJ0J1xyXG5cclxuICAgIHNjb3BlLnNob3dDaGFydCgpIGlmIHNjb3BlLm1ldHJpYy52aWV3ID09ICdjaGFydCdcclxuXHJcbiAgICBzY29wZS4kb24gJ21ldHJpY3M6ZGF0YTp1cGRhdGUnLCAoZXZlbnQsIHRpbWVzdGFtcCwgZGF0YSkgLT5cclxuICAgICAgc2NvcGUudmFsdWUgPSBwYXJzZUZsb2F0KGRhdGFbc2NvcGUubWV0cmljLmlkXSlcclxuXHJcbiAgICAgIHNjb3BlLmRhdGFbMF0udmFsdWVzLnB1c2gge1xyXG4gICAgICAgIHg6IHRpbWVzdGFtcFxyXG4gICAgICAgIHk6IHNjb3BlLnZhbHVlXHJcbiAgICAgIH1cclxuXHJcbiAgICAgIGlmIHNjb3BlLmRhdGFbMF0udmFsdWVzLmxlbmd0aCA+IHNjb3BlLndpbmRvd1xyXG4gICAgICAgIHNjb3BlLmRhdGFbMF0udmFsdWVzLnNoaWZ0KClcclxuXHJcbiAgICAgIHNjb3BlLnNob3dDaGFydCgpIGlmIHNjb3BlLm1ldHJpYy52aWV3ID09ICdjaGFydCdcclxuICAgICAgc2NvcGUuY2hhcnQuY2xlYXJIaWdobGlnaHRzKCkgaWYgc2NvcGUubWV0cmljLnZpZXcgPT0gJ2NoYXJ0J1xyXG4gICAgICBzY29wZS5jaGFydC50b29sdGlwLmhpZGRlbih0cnVlKVxyXG5cclxuICAgIGVsZW1lbnQuZmluZChcIi5tZXRyaWMtdGl0bGVcIikucXRpcCh7XHJcbiAgICAgIGNvbnRlbnQ6IHtcclxuICAgICAgICB0ZXh0OiBzY29wZS5tZXRyaWMuaWRcclxuICAgICAgfSxcclxuICAgICAgcG9zaXRpb246IHtcclxuICAgICAgICBteTogJ2JvdHRvbSBsZWZ0JyxcclxuICAgICAgICBhdDogJ3RvcCBsZWZ0J1xyXG4gICAgICB9LFxyXG4gICAgICBzdHlsZToge1xyXG4gICAgICAgIGNsYXNzZXM6ICdxdGlwLWxpZ2h0IHF0aXAtdGltZWxpbmUtYmFyJ1xyXG4gICAgICB9XHJcbiAgICB9KTsiLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5kaXJlY3RpdmUoJ21ldHJpY3NHcmFwaCcsIGZ1bmN0aW9uKCkge1xuICByZXR1cm4ge1xuICAgIHRlbXBsYXRlOiAnPGRpdiBjbGFzcz1cInBhbmVsIHBhbmVsLWRlZmF1bHQgcGFuZWwtbWV0cmljXCI+IDxkaXYgY2xhc3M9XCJwYW5lbC1oZWFkaW5nXCI+IDxzcGFuIGNsYXNzPVwibWV0cmljLXRpdGxlXCI+e3ttZXRyaWMuaWR9fTwvc3Bhbj4gPGRpdiBjbGFzcz1cImJ1dHRvbnNcIj4gPGRpdiBjbGFzcz1cImJ0bi1ncm91cFwiPiA8YnV0dG9uIHR5cGU9XCJidXR0b25cIiBuZy1jbGFzcz1cIltidG5DbGFzc2VzLCB7YWN0aXZlOiBtZXRyaWMuc2l6ZSAhPSBcXCdiaWdcXCd9XVwiIG5nLWNsaWNrPVwic2V0U2l6ZShcXCdzbWFsbFxcJylcIj5TbWFsbDwvYnV0dG9uPiA8YnV0dG9uIHR5cGU9XCJidXR0b25cIiBuZy1jbGFzcz1cIltidG5DbGFzc2VzLCB7YWN0aXZlOiBtZXRyaWMuc2l6ZSA9PSBcXCdiaWdcXCd9XVwiIG5nLWNsaWNrPVwic2V0U2l6ZShcXCdiaWdcXCcpXCI+QmlnPC9idXR0b24+IDwvZGl2PiA8YSB0aXRsZT1cIlJlbW92ZVwiIGNsYXNzPVwiYnRuIGJ0bi1kZWZhdWx0IGJ0bi14cyByZW1vdmVcIiBuZy1jbGljaz1cInJlbW92ZU1ldHJpYygpXCI+PGkgY2xhc3M9XCJmYSBmYS1jbG9zZVwiIC8+PC9hPiA8L2Rpdj4gPC9kaXY+IDxkaXYgY2xhc3M9XCJwYW5lbC1ib2R5XCI+IDxzdmcgbmctaWY9XCJtZXRyaWMudmlldyA9PSBcXCdjaGFydFxcJ1wiLz4gPGRpdiBuZy1pZj1cIm1ldHJpYy52aWV3ICE9IFxcJ2NoYXJ0XFwnXCI+IDxkaXYgY2xhc3M9XCJtZXRyaWMtbnVtZXJpY1wiIHRpdGxlPVwie3t2YWx1ZSB8IGh1bWFuaXplQ2hhcnROdW1lcmljVGl0bGU6bWV0cmljfX1cIj57e3ZhbHVlIHwgaHVtYW5pemVDaGFydE51bWVyaWM6bWV0cmljfX08L2Rpdj4gPC9kaXY+IDwvZGl2PiA8ZGl2IGNsYXNzPVwiYnV0dG9uc1wiPiA8ZGl2IGNsYXNzPVwiYnRuLWdyb3VwXCI+IDxidXR0b24gdHlwZT1cImJ1dHRvblwiIG5nLWNsYXNzPVwiW2J0bkNsYXNzZXMsIHthY3RpdmU6IG1ldHJpYy52aWV3ID09IFxcJ2NoYXJ0XFwnfV1cIiBuZy1jbGljaz1cInNldFZpZXcoXFwnY2hhcnRcXCcpXCI+Q2hhcnQ8L2J1dHRvbj4gPGJ1dHRvbiB0eXBlPVwiYnV0dG9uXCIgbmctY2xhc3M9XCJbYnRuQ2xhc3Nlcywge2FjdGl2ZTogbWV0cmljLnZpZXcgIT0gXFwnY2hhcnRcXCd9XVwiIG5nLWNsaWNrPVwic2V0VmlldyhcXCdudW1lcmljXFwnKVwiPk51bWVyaWM8L2J1dHRvbj4gPC9kaXY+IDwvZGl2PicsXG4gICAgcmVwbGFjZTogdHJ1ZSxcbiAgICBzY29wZToge1xuICAgICAgbWV0cmljOiBcIj1cIixcbiAgICAgIHdpbmRvdzogXCI9XCIsXG4gICAgICByZW1vdmVNZXRyaWM6IFwiJlwiLFxuICAgICAgc2V0TWV0cmljU2l6ZTogXCI9XCIsXG4gICAgICBzZXRNZXRyaWNWaWV3OiBcIj1cIixcbiAgICAgIGdldFZhbHVlczogXCImXCJcbiAgICB9LFxuICAgIGxpbms6IGZ1bmN0aW9uKHNjb3BlLCBlbGVtZW50LCBhdHRycykge1xuICAgICAgc2NvcGUuYnRuQ2xhc3NlcyA9IFsnYnRuJywgJ2J0bi1kZWZhdWx0JywgJ2J0bi14cyddO1xuICAgICAgc2NvcGUudmFsdWUgPSBudWxsO1xuICAgICAgc2NvcGUuZGF0YSA9IFtcbiAgICAgICAge1xuICAgICAgICAgIHZhbHVlczogc2NvcGUuZ2V0VmFsdWVzKClcbiAgICAgICAgfVxuICAgICAgXTtcbiAgICAgIHNjb3BlLm9wdGlvbnMgPSB7XG4gICAgICAgIHg6IGZ1bmN0aW9uKGQsIGkpIHtcbiAgICAgICAgICByZXR1cm4gZC54O1xuICAgICAgICB9LFxuICAgICAgICB5OiBmdW5jdGlvbihkLCBpKSB7XG4gICAgICAgICAgcmV0dXJuIGQueTtcbiAgICAgICAgfSxcbiAgICAgICAgeFRpY2tGb3JtYXQ6IGZ1bmN0aW9uKGQpIHtcbiAgICAgICAgICByZXR1cm4gZDMudGltZS5mb3JtYXQoJyVIOiVNOiVTJykobmV3IERhdGUoZCkpO1xuICAgICAgICB9LFxuICAgICAgICB5VGlja0Zvcm1hdDogZnVuY3Rpb24oZCkge1xuICAgICAgICAgIHZhciBhYnNELCBmb3VuZCwgcG93LCBzdGVwO1xuICAgICAgICAgIGZvdW5kID0gZmFsc2U7XG4gICAgICAgICAgcG93ID0gMDtcbiAgICAgICAgICBzdGVwID0gMTtcbiAgICAgICAgICBhYnNEID0gTWF0aC5hYnMoZCk7XG4gICAgICAgICAgd2hpbGUgKCFmb3VuZCAmJiBwb3cgPCA1MCkge1xuICAgICAgICAgICAgaWYgKE1hdGgucG93KDEwLCBwb3cpIDw9IGFic0QgJiYgYWJzRCA8IE1hdGgucG93KDEwLCBwb3cgKyBzdGVwKSkge1xuICAgICAgICAgICAgICBmb3VuZCA9IHRydWU7XG4gICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICBwb3cgKz0gc3RlcDtcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGZvdW5kICYmIHBvdyA+IDYpIHtcbiAgICAgICAgICAgIHJldHVybiAoZCAvIE1hdGgucG93KDEwLCBwb3cpKSArIFwiRVwiICsgcG93O1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gXCJcIiArIGQ7XG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgc2NvcGUuc2hvd0NoYXJ0ID0gZnVuY3Rpb24oKSB7XG4gICAgICAgIHJldHVybiBkMy5zZWxlY3QoZWxlbWVudC5maW5kKFwic3ZnXCIpWzBdKS5kYXR1bShzY29wZS5kYXRhKS50cmFuc2l0aW9uKCkuZHVyYXRpb24oMjUwKS5jYWxsKHNjb3BlLmNoYXJ0KTtcbiAgICAgIH07XG4gICAgICBzY29wZS5jaGFydCA9IG52Lm1vZGVscy5saW5lQ2hhcnQoKS5vcHRpb25zKHNjb3BlLm9wdGlvbnMpLnNob3dMZWdlbmQoZmFsc2UpLm1hcmdpbih7XG4gICAgICAgIHRvcDogMTUsXG4gICAgICAgIGxlZnQ6IDYwLFxuICAgICAgICBib3R0b206IDMwLFxuICAgICAgICByaWdodDogMzBcbiAgICAgIH0pO1xuICAgICAgc2NvcGUuY2hhcnQueUF4aXMuc2hvd01heE1pbihmYWxzZSk7XG4gICAgICBzY29wZS5jaGFydC50b29sdGlwLmhpZGVEZWxheSgwKTtcbiAgICAgIHNjb3BlLmNoYXJ0LnRvb2x0aXAuY29udGVudEdlbmVyYXRvcihmdW5jdGlvbihvYmopIHtcbiAgICAgICAgcmV0dXJuIFwiPHA+XCIgKyAoZDMudGltZS5mb3JtYXQoJyVIOiVNOiVTJykobmV3IERhdGUob2JqLnBvaW50LngpKSkgKyBcIiB8IFwiICsgb2JqLnBvaW50LnkgKyBcIjwvcD5cIjtcbiAgICAgIH0pO1xuICAgICAgbnYudXRpbHMud2luZG93UmVzaXplKHNjb3BlLmNoYXJ0LnVwZGF0ZSk7XG4gICAgICBzY29wZS5zZXRTaXplID0gZnVuY3Rpb24oc2l6ZSkge1xuICAgICAgICByZXR1cm4gc2NvcGUuc2V0TWV0cmljU2l6ZShzY29wZS5tZXRyaWMsIHNpemUpO1xuICAgICAgfTtcbiAgICAgIHNjb3BlLnNldFZpZXcgPSBmdW5jdGlvbih2aWV3KSB7XG4gICAgICAgIHNjb3BlLnNldE1ldHJpY1ZpZXcoc2NvcGUubWV0cmljLCB2aWV3KTtcbiAgICAgICAgaWYgKHZpZXcgPT09ICdjaGFydCcpIHtcbiAgICAgICAgICByZXR1cm4gc2NvcGUuc2hvd0NoYXJ0KCk7XG4gICAgICAgIH1cbiAgICAgIH07XG4gICAgICBpZiAoc2NvcGUubWV0cmljLnZpZXcgPT09ICdjaGFydCcpIHtcbiAgICAgICAgc2NvcGUuc2hvd0NoYXJ0KCk7XG4gICAgICB9XG4gICAgICBzY29wZS4kb24oJ21ldHJpY3M6ZGF0YTp1cGRhdGUnLCBmdW5jdGlvbihldmVudCwgdGltZXN0YW1wLCBkYXRhKSB7XG4gICAgICAgIHNjb3BlLnZhbHVlID0gcGFyc2VGbG9hdChkYXRhW3Njb3BlLm1ldHJpYy5pZF0pO1xuICAgICAgICBzY29wZS5kYXRhWzBdLnZhbHVlcy5wdXNoKHtcbiAgICAgICAgICB4OiB0aW1lc3RhbXAsXG4gICAgICAgICAgeTogc2NvcGUudmFsdWVcbiAgICAgICAgfSk7XG4gICAgICAgIGlmIChzY29wZS5kYXRhWzBdLnZhbHVlcy5sZW5ndGggPiBzY29wZS53aW5kb3cpIHtcbiAgICAgICAgICBzY29wZS5kYXRhWzBdLnZhbHVlcy5zaGlmdCgpO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzY29wZS5tZXRyaWMudmlldyA9PT0gJ2NoYXJ0Jykge1xuICAgICAgICAgIHNjb3BlLnNob3dDaGFydCgpO1xuICAgICAgICB9XG4gICAgICAgIGlmIChzY29wZS5tZXRyaWMudmlldyA9PT0gJ2NoYXJ0Jykge1xuICAgICAgICAgIHNjb3BlLmNoYXJ0LmNsZWFySGlnaGxpZ2h0cygpO1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBzY29wZS5jaGFydC50b29sdGlwLmhpZGRlbih0cnVlKTtcbiAgICAgIH0pO1xuICAgICAgcmV0dXJuIGVsZW1lbnQuZmluZChcIi5tZXRyaWMtdGl0bGVcIikucXRpcCh7XG4gICAgICAgIGNvbnRlbnQ6IHtcbiAgICAgICAgICB0ZXh0OiBzY29wZS5tZXRyaWMuaWRcbiAgICAgICAgfSxcbiAgICAgICAgcG9zaXRpb246IHtcbiAgICAgICAgICBteTogJ2JvdHRvbSBsZWZ0JyxcbiAgICAgICAgICBhdDogJ3RvcCBsZWZ0J1xuICAgICAgICB9LFxuICAgICAgICBzdHlsZToge1xuICAgICAgICAgIGNsYXNzZXM6ICdxdGlwLWxpZ2h0IHF0aXAtdGltZWxpbmUtYmFyJ1xuICAgICAgICB9XG4gICAgICB9KTtcbiAgICB9XG4gIH07XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLnNlcnZpY2UgJ01ldHJpY3NTZXJ2aWNlJywgKCRodHRwLCAkcSwgZmxpbmtDb25maWcsICRpbnRlcnZhbCkgLT5cclxuICBAbWV0cmljcyA9IHt9XHJcbiAgQHZhbHVlcyA9IHt9XHJcbiAgQHdhdGNoZWQgPSB7fVxyXG4gIEBvYnNlcnZlciA9IHtcclxuICAgIGpvYmlkOiBudWxsXHJcbiAgICBub2RlaWQ6IG51bGxcclxuICAgIGNhbGxiYWNrOiBudWxsXHJcbiAgfVxyXG5cclxuICBAcmVmcmVzaCA9ICRpbnRlcnZhbCA9PlxyXG4gICAgYW5ndWxhci5mb3JFYWNoIEBtZXRyaWNzLCAodmVydGljZXMsIGpvYmlkKSA9PlxyXG4gICAgICBhbmd1bGFyLmZvckVhY2ggdmVydGljZXMsIChtZXRyaWNzLCBub2RlaWQpID0+XHJcbiAgICAgICAgbmFtZXMgPSBbXVxyXG4gICAgICAgIGFuZ3VsYXIuZm9yRWFjaCBtZXRyaWNzLCAobWV0cmljLCBpbmRleCkgPT5cclxuICAgICAgICAgIG5hbWVzLnB1c2ggbWV0cmljLmlkXHJcblxyXG4gICAgICAgIGlmIG5hbWVzLmxlbmd0aCA+IDBcclxuICAgICAgICAgIEBnZXRNZXRyaWNzKGpvYmlkLCBub2RlaWQsIG5hbWVzKS50aGVuICh2YWx1ZXMpID0+XHJcbiAgICAgICAgICAgIGlmIGpvYmlkID09IEBvYnNlcnZlci5qb2JpZCAmJiBub2RlaWQgPT0gQG9ic2VydmVyLm5vZGVpZFxyXG4gICAgICAgICAgICAgIEBvYnNlcnZlci5jYWxsYmFjayh2YWx1ZXMpIGlmIEBvYnNlcnZlci5jYWxsYmFja1xyXG5cclxuXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgQHJlZ2lzdGVyT2JzZXJ2ZXIgPSAoam9iaWQsIG5vZGVpZCwgY2FsbGJhY2spIC0+XHJcbiAgICBAb2JzZXJ2ZXIuam9iaWQgPSBqb2JpZFxyXG4gICAgQG9ic2VydmVyLm5vZGVpZCA9IG5vZGVpZFxyXG4gICAgQG9ic2VydmVyLmNhbGxiYWNrID0gY2FsbGJhY2tcclxuXHJcbiAgQHVuUmVnaXN0ZXJPYnNlcnZlciA9IC0+XHJcbiAgICBAb2JzZXJ2ZXIgPSB7XHJcbiAgICAgIGpvYmlkOiBudWxsXHJcbiAgICAgIG5vZGVpZDogbnVsbFxyXG4gICAgICBjYWxsYmFjazogbnVsbFxyXG4gICAgfVxyXG5cclxuICBAc2V0dXBNZXRyaWNzID0gKGpvYmlkLCB2ZXJ0aWNlcykgLT5cclxuICAgIEBzZXR1cExTKClcclxuXHJcbiAgICBAd2F0Y2hlZFtqb2JpZF0gPSBbXVxyXG4gICAgYW5ndWxhci5mb3JFYWNoIHZlcnRpY2VzLCAodiwgaykgPT5cclxuICAgICAgQHdhdGNoZWRbam9iaWRdLnB1c2godi5pZCkgaWYgdi5pZFxyXG5cclxuICBAZ2V0V2luZG93ID0gLT5cclxuICAgIDEwMFxyXG5cclxuICBAc2V0dXBMUyA9IC0+XHJcbiAgICBpZiAhc2Vzc2lvblN0b3JhZ2UuZmxpbmtNZXRyaWNzP1xyXG4gICAgICBAc2F2ZVNldHVwKClcclxuXHJcbiAgICBAbWV0cmljcyA9IEpTT04ucGFyc2Uoc2Vzc2lvblN0b3JhZ2UuZmxpbmtNZXRyaWNzKVxyXG5cclxuICBAc2F2ZVNldHVwID0gLT5cclxuICAgIHNlc3Npb25TdG9yYWdlLmZsaW5rTWV0cmljcyA9IEpTT04uc3RyaW5naWZ5KEBtZXRyaWNzKVxyXG5cclxuICBAc2F2ZVZhbHVlID0gKGpvYmlkLCBub2RlaWQsIHZhbHVlKSAtPlxyXG4gICAgdW5sZXNzIEB2YWx1ZXNbam9iaWRdP1xyXG4gICAgICBAdmFsdWVzW2pvYmlkXSA9IHt9XHJcblxyXG4gICAgdW5sZXNzIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0/XHJcbiAgICAgIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0gPSBbXVxyXG5cclxuICAgIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0ucHVzaCh2YWx1ZSlcclxuXHJcbiAgICBpZiBAdmFsdWVzW2pvYmlkXVtub2RlaWRdLmxlbmd0aCA+IEBnZXRXaW5kb3coKVxyXG4gICAgICBAdmFsdWVzW2pvYmlkXVtub2RlaWRdLnNoaWZ0KClcclxuXHJcbiAgQGdldFZhbHVlcyA9IChqb2JpZCwgbm9kZWlkLCBtZXRyaWNpZCkgLT5cclxuICAgIHJldHVybiBbXSB1bmxlc3MgQHZhbHVlc1tqb2JpZF0/XHJcbiAgICByZXR1cm4gW10gdW5sZXNzIEB2YWx1ZXNbam9iaWRdW25vZGVpZF0/XHJcblxyXG4gICAgcmVzdWx0cyA9IFtdXHJcbiAgICBhbmd1bGFyLmZvckVhY2ggQHZhbHVlc1tqb2JpZF1bbm9kZWlkXSwgKHYsIGspID0+XHJcbiAgICAgIGlmIHYudmFsdWVzW21ldHJpY2lkXT9cclxuICAgICAgICByZXN1bHRzLnB1c2gge1xyXG4gICAgICAgICAgeDogdi50aW1lc3RhbXBcclxuICAgICAgICAgIHk6IHYudmFsdWVzW21ldHJpY2lkXVxyXG4gICAgICAgIH1cclxuXHJcbiAgICByZXN1bHRzXHJcblxyXG4gIEBzZXR1cExTRm9yID0gKGpvYmlkLCBub2RlaWQpIC0+XHJcbiAgICBpZiAhQG1ldHJpY3Nbam9iaWRdP1xyXG4gICAgICBAbWV0cmljc1tqb2JpZF0gPSB7fVxyXG5cclxuICAgIGlmICFAbWV0cmljc1tqb2JpZF1bbm9kZWlkXT9cclxuICAgICAgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0gPSBbXVxyXG5cclxuICBAYWRkTWV0cmljID0gKGpvYmlkLCBub2RlaWQsIG1ldHJpY2lkKSAtPlxyXG4gICAgQHNldHVwTFNGb3Ioam9iaWQsIG5vZGVpZClcclxuXHJcbiAgICBAbWV0cmljc1tqb2JpZF1bbm9kZWlkXS5wdXNoKHtpZDogbWV0cmljaWQsIHNpemU6ICdzbWFsbCcsIHZpZXc6ICdjaGFydCd9KVxyXG5cclxuICAgIEBzYXZlU2V0dXAoKVxyXG5cclxuICBAcmVtb3ZlTWV0cmljID0gKGpvYmlkLCBub2RlaWQsIG1ldHJpYykgPT5cclxuICAgIGlmIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdP1xyXG4gICAgICBpID0gQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZihtZXRyaWMpXHJcbiAgICAgIGkgPSBfLmZpbmRJbmRleChAbWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgeyBpZDogbWV0cmljIH0pIGlmIGkgPT0gLTFcclxuXHJcbiAgICAgIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdLnNwbGljZShpLCAxKSBpZiBpICE9IC0xXHJcblxyXG4gICAgICBAc2F2ZVNldHVwKClcclxuXHJcbiAgQHNldE1ldHJpY1NpemUgPSAoam9iaWQsIG5vZGVpZCwgbWV0cmljLCBzaXplKSA9PlxyXG4gICAgaWYgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0/XHJcbiAgICAgIGkgPSBAbWV0cmljc1tqb2JpZF1bbm9kZWlkXS5pbmRleE9mKG1ldHJpYy5pZClcclxuICAgICAgaSA9IF8uZmluZEluZGV4KEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdLCB7IGlkOiBtZXRyaWMuaWQgfSkgaWYgaSA9PSAtMVxyXG5cclxuICAgICAgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF1baV0gPSB7IGlkOiBtZXRyaWMuaWQsIHNpemU6IHNpemUsIHZpZXc6IG1ldHJpYy52aWV3IH0gaWYgaSAhPSAtMVxyXG5cclxuICAgICAgQHNhdmVTZXR1cCgpXHJcblxyXG4gIEBzZXRNZXRyaWNWaWV3ID0gKGpvYmlkLCBub2RlaWQsIG1ldHJpYywgdmlldykgPT5cclxuICAgIGlmIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdP1xyXG4gICAgICBpID0gQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZihtZXRyaWMuaWQpXHJcbiAgICAgIGkgPSBfLmZpbmRJbmRleChAbWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgeyBpZDogbWV0cmljLmlkIH0pIGlmIGkgPT0gLTFcclxuXHJcbiAgICAgIEBtZXRyaWNzW2pvYmlkXVtub2RlaWRdW2ldID0geyBpZDogbWV0cmljLmlkLCBzaXplOiBtZXRyaWMuc2l6ZSwgdmlldzogdmlldyB9IGlmIGkgIT0gLTFcclxuXHJcbiAgICAgIEBzYXZlU2V0dXAoKVxyXG5cclxuICBAb3JkZXJNZXRyaWNzID0gKGpvYmlkLCBub2RlaWQsIGl0ZW0sIGluZGV4KSAtPlxyXG4gICAgQHNldHVwTFNGb3Ioam9iaWQsIG5vZGVpZClcclxuXHJcbiAgICBhbmd1bGFyLmZvckVhY2ggQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0sICh2LCBrKSA9PlxyXG4gICAgICBpZiB2LmlkID09IGl0ZW0uaWRcclxuICAgICAgICBAbWV0cmljc1tqb2JpZF1bbm9kZWlkXS5zcGxpY2UoaywgMSlcclxuICAgICAgICBpZiBrIDwgaW5kZXhcclxuICAgICAgICAgIGluZGV4ID0gaW5kZXggLSAxXHJcblxyXG4gICAgQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uc3BsaWNlKGluZGV4LCAwLCBpdGVtKVxyXG5cclxuICAgIEBzYXZlU2V0dXAoKVxyXG5cclxuICBAZ2V0TWV0cmljc1NldHVwID0gKGpvYmlkLCBub2RlaWQpID0+XHJcbiAgICB7XHJcbiAgICAgIG5hbWVzOiBfLm1hcChAbWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgKHZhbHVlKSA9PlxyXG4gICAgICAgIGlmIF8uaXNTdHJpbmcodmFsdWUpIHRoZW4geyBpZDogdmFsdWUsIHNpemU6IFwic21hbGxcIiwgdmlldzogXCJjaGFydFwiIH0gZWxzZSB2YWx1ZVxyXG4gICAgICApXHJcbiAgICB9XHJcblxyXG4gIEBnZXRBdmFpbGFibGVNZXRyaWNzID0gKGpvYmlkLCBub2RlaWQpID0+XHJcbiAgICBAc2V0dXBMU0Zvcihqb2JpZCwgbm9kZWlkKVxyXG5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzL1wiICsgbm9kZWlkICsgXCIvbWV0cmljc1wiXHJcbiAgICAuc3VjY2VzcyAoZGF0YSkgPT5cclxuICAgICAgcmVzdWx0cyA9IFtdXHJcbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAodiwgaykgPT5cclxuICAgICAgICBpID0gQG1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZih2LmlkKVxyXG4gICAgICAgIGkgPSBfLmZpbmRJbmRleChAbWV0cmljc1tqb2JpZF1bbm9kZWlkXSwgeyBpZDogdi5pZCB9KSBpZiBpID09IC0xXHJcblxyXG4gICAgICAgIGlmIGkgPT0gLTFcclxuICAgICAgICAgIHJlc3VsdHMucHVzaCh2KVxyXG5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShyZXN1bHRzKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldEFsbEF2YWlsYWJsZU1ldHJpY3MgPSAoam9iaWQsIG5vZGVpZCkgPT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImpvYnMvXCIgKyBqb2JpZCArIFwiL3ZlcnRpY2VzL1wiICsgbm9kZWlkICsgXCIvbWV0cmljc1wiXHJcbiAgICAuc3VjY2VzcyAoZGF0YSkgPT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldE1ldHJpY3MgPSAoam9iaWQsIG5vZGVpZCwgbWV0cmljSWRzKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgaWRzID0gbWV0cmljSWRzLmpvaW4oXCIsXCIpXHJcblxyXG4gICAgJGh0dHAuZ2V0IGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXMvXCIgKyBub2RlaWQgKyBcIi9tZXRyaWNzP2dldD1cIiArIGlkc1xyXG4gICAgLnN1Y2Nlc3MgKGRhdGEpID0+XHJcbiAgICAgIHJlc3VsdCA9IHt9XHJcbiAgICAgIGFuZ3VsYXIuZm9yRWFjaCBkYXRhLCAodiwgaykgLT5cclxuICAgICAgICByZXN1bHRbdi5pZF0gPSBwYXJzZUludCh2LnZhbHVlKVxyXG5cclxuICAgICAgbmV3VmFsdWUgPSB7XHJcbiAgICAgICAgdGltZXN0YW1wOiBEYXRlLm5vdygpXHJcbiAgICAgICAgdmFsdWVzOiByZXN1bHRcclxuICAgICAgfVxyXG4gICAgICBAc2F2ZVZhbHVlKGpvYmlkLCBub2RlaWQsIG5ld1ZhbHVlKVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKG5ld1ZhbHVlKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQHNldHVwTFMoKVxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ01ldHJpY3NTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsICRxLCBmbGlua0NvbmZpZywgJGludGVydmFsKSB7XG4gIHRoaXMubWV0cmljcyA9IHt9O1xuICB0aGlzLnZhbHVlcyA9IHt9O1xuICB0aGlzLndhdGNoZWQgPSB7fTtcbiAgdGhpcy5vYnNlcnZlciA9IHtcbiAgICBqb2JpZDogbnVsbCxcbiAgICBub2RlaWQ6IG51bGwsXG4gICAgY2FsbGJhY2s6IG51bGxcbiAgfTtcbiAgdGhpcy5yZWZyZXNoID0gJGludGVydmFsKChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbigpIHtcbiAgICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2goX3RoaXMubWV0cmljcywgZnVuY3Rpb24odmVydGljZXMsIGpvYmlkKSB7XG4gICAgICAgIHJldHVybiBhbmd1bGFyLmZvckVhY2godmVydGljZXMsIGZ1bmN0aW9uKG1ldHJpY3MsIG5vZGVpZCkge1xuICAgICAgICAgIHZhciBuYW1lcztcbiAgICAgICAgICBuYW1lcyA9IFtdO1xuICAgICAgICAgIGFuZ3VsYXIuZm9yRWFjaChtZXRyaWNzLCBmdW5jdGlvbihtZXRyaWMsIGluZGV4KSB7XG4gICAgICAgICAgICByZXR1cm4gbmFtZXMucHVzaChtZXRyaWMuaWQpO1xuICAgICAgICAgIH0pO1xuICAgICAgICAgIGlmIChuYW1lcy5sZW5ndGggPiAwKSB7XG4gICAgICAgICAgICByZXR1cm4gX3RoaXMuZ2V0TWV0cmljcyhqb2JpZCwgbm9kZWlkLCBuYW1lcykudGhlbihmdW5jdGlvbih2YWx1ZXMpIHtcbiAgICAgICAgICAgICAgaWYgKGpvYmlkID09PSBfdGhpcy5vYnNlcnZlci5qb2JpZCAmJiBub2RlaWQgPT09IF90aGlzLm9ic2VydmVyLm5vZGVpZCkge1xuICAgICAgICAgICAgICAgIGlmIChfdGhpcy5vYnNlcnZlci5jYWxsYmFjaykge1xuICAgICAgICAgICAgICAgICAgcmV0dXJuIF90aGlzLm9ic2VydmVyLmNhbGxiYWNrKHZhbHVlcyk7XG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgfSk7XG4gICAgfTtcbiAgfSkodGhpcyksIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHRoaXMucmVnaXN0ZXJPYnNlcnZlciA9IGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQsIGNhbGxiYWNrKSB7XG4gICAgdGhpcy5vYnNlcnZlci5qb2JpZCA9IGpvYmlkO1xuICAgIHRoaXMub2JzZXJ2ZXIubm9kZWlkID0gbm9kZWlkO1xuICAgIHJldHVybiB0aGlzLm9ic2VydmVyLmNhbGxiYWNrID0gY2FsbGJhY2s7XG4gIH07XG4gIHRoaXMudW5SZWdpc3Rlck9ic2VydmVyID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIHRoaXMub2JzZXJ2ZXIgPSB7XG4gICAgICBqb2JpZDogbnVsbCxcbiAgICAgIG5vZGVpZDogbnVsbCxcbiAgICAgIGNhbGxiYWNrOiBudWxsXG4gICAgfTtcbiAgfTtcbiAgdGhpcy5zZXR1cE1ldHJpY3MgPSBmdW5jdGlvbihqb2JpZCwgdmVydGljZXMpIHtcbiAgICB0aGlzLnNldHVwTFMoKTtcbiAgICB0aGlzLndhdGNoZWRbam9iaWRdID0gW107XG4gICAgcmV0dXJuIGFuZ3VsYXIuZm9yRWFjaCh2ZXJ0aWNlcywgKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgICByZXR1cm4gZnVuY3Rpb24odiwgaykge1xuICAgICAgICBpZiAodi5pZCkge1xuICAgICAgICAgIHJldHVybiBfdGhpcy53YXRjaGVkW2pvYmlkXS5wdXNoKHYuaWQpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgfTtcbiAgdGhpcy5nZXRXaW5kb3cgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gMTAwO1xuICB9O1xuICB0aGlzLnNldHVwTFMgPSBmdW5jdGlvbigpIHtcbiAgICBpZiAoc2Vzc2lvblN0b3JhZ2UuZmxpbmtNZXRyaWNzID09IG51bGwpIHtcbiAgICAgIHRoaXMuc2F2ZVNldHVwKCk7XG4gICAgfVxuICAgIHJldHVybiB0aGlzLm1ldHJpY3MgPSBKU09OLnBhcnNlKHNlc3Npb25TdG9yYWdlLmZsaW5rTWV0cmljcyk7XG4gIH07XG4gIHRoaXMuc2F2ZVNldHVwID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIHNlc3Npb25TdG9yYWdlLmZsaW5rTWV0cmljcyA9IEpTT04uc3RyaW5naWZ5KHRoaXMubWV0cmljcyk7XG4gIH07XG4gIHRoaXMuc2F2ZVZhbHVlID0gZnVuY3Rpb24oam9iaWQsIG5vZGVpZCwgdmFsdWUpIHtcbiAgICBpZiAodGhpcy52YWx1ZXNbam9iaWRdID09IG51bGwpIHtcbiAgICAgIHRoaXMudmFsdWVzW2pvYmlkXSA9IHt9O1xuICAgIH1cbiAgICBpZiAodGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0gPT0gbnVsbCkge1xuICAgICAgdGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0gPSBbXTtcbiAgICB9XG4gICAgdGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0ucHVzaCh2YWx1ZSk7XG4gICAgaWYgKHRoaXMudmFsdWVzW2pvYmlkXVtub2RlaWRdLmxlbmd0aCA+IHRoaXMuZ2V0V2luZG93KCkpIHtcbiAgICAgIHJldHVybiB0aGlzLnZhbHVlc1tqb2JpZF1bbm9kZWlkXS5zaGlmdCgpO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5nZXRWYWx1ZXMgPSBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWNpZCkge1xuICAgIHZhciByZXN1bHRzO1xuICAgIGlmICh0aGlzLnZhbHVlc1tqb2JpZF0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cbiAgICBpZiAodGhpcy52YWx1ZXNbam9iaWRdW25vZGVpZF0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIFtdO1xuICAgIH1cbiAgICByZXN1bHRzID0gW107XG4gICAgYW5ndWxhci5mb3JFYWNoKHRoaXMudmFsdWVzW2pvYmlkXVtub2RlaWRdLCAoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbih2LCBrKSB7XG4gICAgICAgIGlmICh2LnZhbHVlc1ttZXRyaWNpZF0gIT0gbnVsbCkge1xuICAgICAgICAgIHJldHVybiByZXN1bHRzLnB1c2goe1xuICAgICAgICAgICAgeDogdi50aW1lc3RhbXAsXG4gICAgICAgICAgICB5OiB2LnZhbHVlc1ttZXRyaWNpZF1cbiAgICAgICAgICB9KTtcbiAgICAgICAgfVxuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgcmV0dXJuIHJlc3VsdHM7XG4gIH07XG4gIHRoaXMuc2V0dXBMU0ZvciA9IGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQpIHtcbiAgICBpZiAodGhpcy5tZXRyaWNzW2pvYmlkXSA9PSBudWxsKSB7XG4gICAgICB0aGlzLm1ldHJpY3Nbam9iaWRdID0ge307XG4gICAgfVxuICAgIGlmICh0aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0gPT0gbnVsbCkge1xuICAgICAgcmV0dXJuIHRoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSA9IFtdO1xuICAgIH1cbiAgfTtcbiAgdGhpcy5hZGRNZXRyaWMgPSBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWNpZCkge1xuICAgIHRoaXMuc2V0dXBMU0Zvcihqb2JpZCwgbm9kZWlkKTtcbiAgICB0aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0ucHVzaCh7XG4gICAgICBpZDogbWV0cmljaWQsXG4gICAgICBzaXplOiAnc21hbGwnLFxuICAgICAgdmlldzogJ2NoYXJ0J1xuICAgIH0pO1xuICAgIHJldHVybiB0aGlzLnNhdmVTZXR1cCgpO1xuICB9O1xuICB0aGlzLnJlbW92ZU1ldHJpYyA9IChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWMpIHtcbiAgICAgIHZhciBpO1xuICAgICAgaWYgKF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0gIT0gbnVsbCkge1xuICAgICAgICBpID0gX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXS5pbmRleE9mKG1ldHJpYyk7XG4gICAgICAgIGlmIChpID09PSAtMSkge1xuICAgICAgICAgIGkgPSBfLmZpbmRJbmRleChfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLCB7XG4gICAgICAgICAgICBpZDogbWV0cmljXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGkgIT09IC0xKSB7XG4gICAgICAgICAgX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXS5zcGxpY2UoaSwgMSk7XG4gICAgICAgIH1cbiAgICAgICAgcmV0dXJuIF90aGlzLnNhdmVTZXR1cCgpO1xuICAgICAgfVxuICAgIH07XG4gIH0pKHRoaXMpO1xuICB0aGlzLnNldE1ldHJpY1NpemUgPSAoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICByZXR1cm4gZnVuY3Rpb24oam9iaWQsIG5vZGVpZCwgbWV0cmljLCBzaXplKSB7XG4gICAgICB2YXIgaTtcbiAgICAgIGlmIChfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdICE9IG51bGwpIHtcbiAgICAgICAgaSA9IF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0uaW5kZXhPZihtZXRyaWMuaWQpO1xuICAgICAgICBpZiAoaSA9PT0gLTEpIHtcbiAgICAgICAgICBpID0gXy5maW5kSW5kZXgoX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSwge1xuICAgICAgICAgICAgaWQ6IG1ldHJpYy5pZFxuICAgICAgICAgIH0pO1xuICAgICAgICB9XG4gICAgICAgIGlmIChpICE9PSAtMSkge1xuICAgICAgICAgIF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF1baV0gPSB7XG4gICAgICAgICAgICBpZDogbWV0cmljLmlkLFxuICAgICAgICAgICAgc2l6ZTogc2l6ZSxcbiAgICAgICAgICAgIHZpZXc6IG1ldHJpYy52aWV3XG4gICAgICAgICAgfTtcbiAgICAgICAgfVxuICAgICAgICByZXR1cm4gX3RoaXMuc2F2ZVNldHVwKCk7XG4gICAgICB9XG4gICAgfTtcbiAgfSkodGhpcyk7XG4gIHRoaXMuc2V0TWV0cmljVmlldyA9IChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBtZXRyaWMsIHZpZXcpIHtcbiAgICAgIHZhciBpO1xuICAgICAgaWYgKF90aGlzLm1ldHJpY3Nbam9iaWRdW25vZGVpZF0gIT0gbnVsbCkge1xuICAgICAgICBpID0gX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXS5pbmRleE9mKG1ldHJpYy5pZCk7XG4gICAgICAgIGlmIChpID09PSAtMSkge1xuICAgICAgICAgIGkgPSBfLmZpbmRJbmRleChfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLCB7XG4gICAgICAgICAgICBpZDogbWV0cmljLmlkXG4gICAgICAgICAgfSk7XG4gICAgICAgIH1cbiAgICAgICAgaWYgKGkgIT09IC0xKSB7XG4gICAgICAgICAgX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXVtpXSA9IHtcbiAgICAgICAgICAgIGlkOiBtZXRyaWMuaWQsXG4gICAgICAgICAgICBzaXplOiBtZXRyaWMuc2l6ZSxcbiAgICAgICAgICAgIHZpZXc6IHZpZXdcbiAgICAgICAgICB9O1xuICAgICAgICB9XG4gICAgICAgIHJldHVybiBfdGhpcy5zYXZlU2V0dXAoKTtcbiAgICAgIH1cbiAgICB9O1xuICB9KSh0aGlzKTtcbiAgdGhpcy5vcmRlck1ldHJpY3MgPSBmdW5jdGlvbihqb2JpZCwgbm9kZWlkLCBpdGVtLCBpbmRleCkge1xuICAgIHRoaXMuc2V0dXBMU0Zvcihqb2JpZCwgbm9kZWlkKTtcbiAgICBhbmd1bGFyLmZvckVhY2godGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLCAoZnVuY3Rpb24oX3RoaXMpIHtcbiAgICAgIHJldHVybiBmdW5jdGlvbih2LCBrKSB7XG4gICAgICAgIGlmICh2LmlkID09PSBpdGVtLmlkKSB7XG4gICAgICAgICAgX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXS5zcGxpY2UoaywgMSk7XG4gICAgICAgICAgaWYgKGsgPCBpbmRleCkge1xuICAgICAgICAgICAgcmV0dXJuIGluZGV4ID0gaW5kZXggLSAxO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICB9KSh0aGlzKSk7XG4gICAgdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLnNwbGljZShpbmRleCwgMCwgaXRlbSk7XG4gICAgcmV0dXJuIHRoaXMuc2F2ZVNldHVwKCk7XG4gIH07XG4gIHRoaXMuZ2V0TWV0cmljc1NldHVwID0gKGZ1bmN0aW9uKF90aGlzKSB7XG4gICAgcmV0dXJuIGZ1bmN0aW9uKGpvYmlkLCBub2RlaWQpIHtcbiAgICAgIHJldHVybiB7XG4gICAgICAgIG5hbWVzOiBfLm1hcChfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLCBmdW5jdGlvbih2YWx1ZSkge1xuICAgICAgICAgIGlmIChfLmlzU3RyaW5nKHZhbHVlKSkge1xuICAgICAgICAgICAgcmV0dXJuIHtcbiAgICAgICAgICAgICAgaWQ6IHZhbHVlLFxuICAgICAgICAgICAgICBzaXplOiBcInNtYWxsXCIsXG4gICAgICAgICAgICAgIHZpZXc6IFwiY2hhcnRcIlxuICAgICAgICAgICAgfTtcbiAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgcmV0dXJuIHZhbHVlO1xuICAgICAgICAgIH1cbiAgICAgICAgfSlcbiAgICAgIH07XG4gICAgfTtcbiAgfSkodGhpcyk7XG4gIHRoaXMuZ2V0QXZhaWxhYmxlTWV0cmljcyA9IChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbihqb2JpZCwgbm9kZWlkKSB7XG4gICAgICB2YXIgZGVmZXJyZWQ7XG4gICAgICBfdGhpcy5zZXR1cExTRm9yKGpvYmlkLCBub2RlaWQpO1xuICAgICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiam9icy9cIiArIGpvYmlkICsgXCIvdmVydGljZXMvXCIgKyBub2RlaWQgKyBcIi9tZXRyaWNzXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSkge1xuICAgICAgICB2YXIgcmVzdWx0cztcbiAgICAgICAgcmVzdWx0cyA9IFtdO1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YSwgZnVuY3Rpb24odiwgaykge1xuICAgICAgICAgIHZhciBpO1xuICAgICAgICAgIGkgPSBfdGhpcy5tZXRyaWNzW2pvYmlkXVtub2RlaWRdLmluZGV4T2Yodi5pZCk7XG4gICAgICAgICAgaWYgKGkgPT09IC0xKSB7XG4gICAgICAgICAgICBpID0gXy5maW5kSW5kZXgoX3RoaXMubWV0cmljc1tqb2JpZF1bbm9kZWlkXSwge1xuICAgICAgICAgICAgICBpZDogdi5pZFxuICAgICAgICAgICAgfSk7XG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChpID09PSAtMSkge1xuICAgICAgICAgICAgcmV0dXJuIHJlc3VsdHMucHVzaCh2KTtcbiAgICAgICAgICB9XG4gICAgICAgIH0pO1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShyZXN1bHRzKTtcbiAgICAgIH0pO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gICAgfTtcbiAgfSkodGhpcyk7XG4gIHRoaXMuZ2V0QWxsQXZhaWxhYmxlTWV0cmljcyA9IChmdW5jdGlvbihfdGhpcykge1xuICAgIHJldHVybiBmdW5jdGlvbihqb2JpZCwgbm9kZWlkKSB7XG4gICAgICB2YXIgZGVmZXJyZWQ7XG4gICAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0aWNlcy9cIiArIG5vZGVpZCArIFwiL21ldHJpY3NcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhKSB7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgICAgfSk7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgICB9O1xuICB9KSh0aGlzKTtcbiAgdGhpcy5nZXRNZXRyaWNzID0gZnVuY3Rpb24oam9iaWQsIG5vZGVpZCwgbWV0cmljSWRzKSB7XG4gICAgdmFyIGRlZmVycmVkLCBpZHM7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgIGlkcyA9IG1ldHJpY0lkcy5qb2luKFwiLFwiKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqb2JzL1wiICsgam9iaWQgKyBcIi92ZXJ0aWNlcy9cIiArIG5vZGVpZCArIFwiL21ldHJpY3M/Z2V0PVwiICsgaWRzKS5zdWNjZXNzKChmdW5jdGlvbihfdGhpcykge1xuICAgICAgcmV0dXJuIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgdmFyIG5ld1ZhbHVlLCByZXN1bHQ7XG4gICAgICAgIHJlc3VsdCA9IHt9O1xuICAgICAgICBhbmd1bGFyLmZvckVhY2goZGF0YSwgZnVuY3Rpb24odiwgaykge1xuICAgICAgICAgIHJldHVybiByZXN1bHRbdi5pZF0gPSBwYXJzZUludCh2LnZhbHVlKTtcbiAgICAgICAgfSk7XG4gICAgICAgIG5ld1ZhbHVlID0ge1xuICAgICAgICAgIHRpbWVzdGFtcDogRGF0ZS5ub3coKSxcbiAgICAgICAgICB2YWx1ZXM6IHJlc3VsdFxuICAgICAgICB9O1xuICAgICAgICBfdGhpcy5zYXZlVmFsdWUoam9iaWQsIG5vZGVpZCwgbmV3VmFsdWUpO1xuICAgICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShuZXdWYWx1ZSk7XG4gICAgICB9O1xuICAgIH0pKHRoaXMpKTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5zZXR1cExTKCk7XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXHJcbiMgTGljZW5zZWQgdG8gdGhlIEFwYWNoZSBTb2Z0d2FyZSBGb3VuZGF0aW9uIChBU0YpIHVuZGVyIG9uZVxyXG4jIG9yIG1vcmUgY29udHJpYnV0b3IgbGljZW5zZSBhZ3JlZW1lbnRzLiAgU2VlIHRoZSBOT1RJQ0UgZmlsZVxyXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXHJcbiMgcmVnYXJkaW5nIGNvcHlyaWdodCBvd25lcnNoaXAuICBUaGUgQVNGIGxpY2Vuc2VzIHRoaXMgZmlsZVxyXG4jIHRvIHlvdSB1bmRlciB0aGUgQXBhY2hlIExpY2Vuc2UsIFZlcnNpb24gMi4wICh0aGVcclxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcclxuIyB3aXRoIHRoZSBMaWNlbnNlLiAgWW91IG1heSBvYnRhaW4gYSBjb3B5IG9mIHRoZSBMaWNlbnNlIGF0XHJcbiNcclxuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXHJcbiNcclxuIyBVbmxlc3MgcmVxdWlyZWQgYnkgYXBwbGljYWJsZSBsYXcgb3IgYWdyZWVkIHRvIGluIHdyaXRpbmcsIHNvZnR3YXJlXHJcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxyXG4jIFdJVEhPVVQgV0FSUkFOVElFUyBPUiBDT05ESVRJT05TIE9GIEFOWSBLSU5ELCBlaXRoZXIgZXhwcmVzcyBvciBpbXBsaWVkLlxyXG4jIFNlZSB0aGUgTGljZW5zZSBmb3IgdGhlIHNwZWNpZmljIGxhbmd1YWdlIGdvdmVybmluZyBwZXJtaXNzaW9ucyBhbmRcclxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cclxuI1xyXG5cclxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcclxuXHJcbi5jb250cm9sbGVyICdKb2JTdWJtaXRDb250cm9sbGVyJywgKCRzY29wZSwgSm9iU3VibWl0U2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZywgJHN0YXRlLCAkbG9jYXRpb24pIC0+XHJcbiAgJHNjb3BlLnlhcm4gPSAkbG9jYXRpb24uYWJzVXJsKCkuaW5kZXhPZihcIi9wcm94eS9hcHBsaWNhdGlvbl9cIikgIT0gLTFcclxuICAkc2NvcGUubG9hZExpc3QgPSAoKSAtPlxyXG4gICAgSm9iU3VibWl0U2VydmljZS5sb2FkSmFyTGlzdCgpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAgICRzY29wZS5hZGRyZXNzID0gZGF0YS5hZGRyZXNzXHJcbiAgICAgIGlmIGRhdGEuZXJyb3I/XHJcbiAgICAgICAgJHNjb3BlLm5vYWNjZXNzID0gZGF0YS5lcnJvclxyXG4gICAgICBlbHNlIGlmIGRhdGEuZXJyb3JzP1xyXG4gICAgICAgICRzY29wZS5ub2FjY2VzcyA9IGRhdGEuZXJyb3JzWzBdXHJcbiAgICAgICRzY29wZS5qYXJzID0gZGF0YS5maWxlc1xyXG5cclxuICAkc2NvcGUuZGVmYXVsdFN0YXRlID0gKCkgLT5cclxuICAgICRzY29wZS5wbGFuID0gbnVsbFxyXG4gICAgJHNjb3BlLmVycm9yID0gbnVsbFxyXG4gICAgJHNjb3BlLnN0YXRlID0ge1xyXG4gICAgICBzZWxlY3RlZDogbnVsbCxcclxuICAgICAgcGFyYWxsZWxpc206IFwiXCIsXHJcbiAgICAgIHNhdmVwb2ludFBhdGg6IFwiXCIsXHJcbiAgICAgIGFsbG93Tm9uUmVzdG9yZWRTdGF0ZTogZmFsc2VcclxuICAgICAgJ2VudHJ5LWNsYXNzJzogXCJcIixcclxuICAgICAgJ3Byb2dyYW0tYXJncyc6IFwiXCIsXHJcbiAgICAgICdwbGFuLWJ1dHRvbic6IFwiU2hvdyBQbGFuXCIsXHJcbiAgICAgICdzdWJtaXQtYnV0dG9uJzogXCJTdWJtaXRcIixcclxuICAgICAgJ2FjdGlvbi10aW1lJzogMFxyXG4gICAgfVxyXG5cclxuICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcclxuICAkc2NvcGUudXBsb2FkZXIgPSB7fVxyXG4gICRzY29wZS5sb2FkTGlzdCgpXHJcblxyXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cclxuICAgICRzY29wZS5sb2FkTGlzdCgpXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxyXG5cclxuICAkc2NvcGUuc2VsZWN0SmFyID0gKGlkKSAtPlxyXG4gICAgaWYgJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09IGlkXHJcbiAgICAgICRzY29wZS5kZWZhdWx0U3RhdGUoKVxyXG4gICAgZWxzZVxyXG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKClcclxuICAgICAgJHNjb3BlLnN0YXRlLnNlbGVjdGVkID0gaWRcclxuXHJcbiAgJHNjb3BlLmRlbGV0ZUphciA9IChldmVudCwgaWQpIC0+XHJcbiAgICBpZiAkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPT0gaWRcclxuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpXHJcbiAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1yZW1vdmVcIikuYWRkQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIilcclxuICAgIEpvYlN1Ym1pdFNlcnZpY2UuZGVsZXRlSmFyKGlkKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICBhbmd1bGFyLmVsZW1lbnQoZXZlbnQuY3VycmVudFRhcmdldCkucmVtb3ZlQ2xhc3MoXCJmYS1zcGluIGZhLXNwaW5uZXJcIikuYWRkQ2xhc3MoXCJmYS1yZW1vdmVcIilcclxuICAgICAgaWYgZGF0YS5lcnJvcj9cclxuICAgICAgICBhbGVydChkYXRhLmVycm9yKVxyXG4gICAgICBlbHNlIGlmIGRhdGEuZXJyb3JzP1xyXG4gICAgICAgIGFsZXJ0KGRhdGEuZXJyb3JzWzBdKVxyXG5cclxuICAkc2NvcGUubG9hZEVudHJ5Q2xhc3MgPSAobmFtZSkgLT5cclxuICAgICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXSA9IG5hbWVcclxuXHJcbiAgJHNjb3BlLmdldFBsYW4gPSAoKSAtPlxyXG4gICAgaWYgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID09IFwiU2hvdyBQbGFuXCJcclxuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKClcclxuICAgICAgJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddID0gYWN0aW9uXHJcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIlxyXG4gICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIkdldHRpbmcgUGxhblwiXHJcbiAgICAgICRzY29wZS5lcnJvciA9IG51bGxcclxuICAgICAgJHNjb3BlLnBsYW4gPSBudWxsXHJcblxyXG4gICAgICBxdWVyeVBhcmFtZXRlcnMgPSB7fVxyXG5cclxuICAgICAgaWYgJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddXHJcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydlbnRyeS1jbGFzcyddID0gJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddXHJcblxyXG4gICAgICBpZiAkc2NvcGUuc3RhdGUucGFyYWxsZWxpc21cclxuICAgICAgICBxdWVyeVBhcmFtZXRlcnNbJ3BhcmFsbGVsaXNtJ10gPSAkc2NvcGUuc3RhdGVbJ3BhcmFsbGVsaXNtJ11cclxuXHJcbiAgICAgIGlmICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ11cclxuICAgICAgICBxdWVyeVBhcmFtZXRlcnNbJ3Byb2dyYW0tYXJncyddID0gJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxyXG5cclxuICAgICAgSm9iU3VibWl0U2VydmljZS5nZXRQbGFuKFxyXG4gICAgICAgICRzY29wZS5zdGF0ZS5zZWxlY3RlZCwgcXVlcnlQYXJhbWV0ZXJzXHJcbiAgICAgICkudGhlbiAoZGF0YSkgLT5cclxuICAgICAgICBpZiBhY3Rpb24gPT0gJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddXHJcbiAgICAgICAgICAkc2NvcGUuc3RhdGVbJ3BsYW4tYnV0dG9uJ10gPSBcIlNob3cgUGxhblwiXHJcbiAgICAgICAgICBpZiBkYXRhLmVycm9yP1xyXG4gICAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yXHJcbiAgICAgICAgICBlbHNlIGlmIGRhdGEuZXJyb3JzP1xyXG4gICAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yc1swXVxyXG4gICAgICAgICAgJHNjb3BlLnBsYW4gPSBkYXRhLnBsYW5cclxuICAgICAgLmNhdGNoIChlcnIpIC0+XHJcbiAgICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIlxyXG4gICAgICAgICRzY29wZS5lcnJvciA9IGVyclxyXG5cclxuICAkc2NvcGUucnVuSm9iID0gKCkgLT5cclxuICAgIGlmICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID09IFwiU3VibWl0XCJcclxuICAgICAgYWN0aW9uID0gbmV3IERhdGUoKS5nZXRUaW1lKClcclxuICAgICAgJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddID0gYWN0aW9uXHJcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXR0aW5nXCJcclxuICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIlxyXG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsXHJcblxyXG4gICAgICByZXF1ZXN0ID0ge31cclxuICAgICAgIyBsZWdhY3kgY29tcGF0aWJpbGl0eVxyXG4gICAgICBxdWVyeVBhcmFtZXRlcnMgPSB7fVxyXG5cclxuICAgICAgaWYgJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddXHJcbiAgICAgICAgcmVxdWVzdFsnZW50cnlDbGFzcyddID0gJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddXHJcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydlbnRyeS1jbGFzcyddID0gJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddXHJcblxyXG4gICAgICBpZiAkc2NvcGUuc3RhdGUucGFyYWxsZWxpc21cclxuICAgICAgICByZXF1ZXN0WydwYXJhbGxlbGlzbSddID0gJHNjb3BlLnN0YXRlWydwYXJhbGxlbGlzbSddXHJcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydwYXJhbGxlbGlzbSddID0gJHNjb3BlLnN0YXRlWydwYXJhbGxlbGlzbSddXHJcblxyXG4gICAgICBpZiAkc2NvcGUuc3RhdGVbJ3Byb2dyYW0tYXJncyddXHJcbiAgICAgICAgcmVxdWVzdFsncHJvZ3JhbUFyZ3MnXSA9ICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ11cclxuICAgICAgICBxdWVyeVBhcmFtZXRlcnNbJ3Byb2dyYW0tYXJncyddID0gJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXVxyXG5cclxuICAgICAgaWYgJHNjb3BlLnN0YXRlWydzYXZlcG9pbnRQYXRoJ11cclxuICAgICAgICByZXF1ZXN0WydzYXZlcG9pbnRQYXRoJ10gPSAkc2NvcGUuc3RhdGVbJ3NhdmVwb2ludFBhdGgnXVxyXG4gICAgICAgIHF1ZXJ5UGFyYW1ldGVyc1snc2F2ZXBvaW50UGF0aCddID0gJHNjb3BlLnN0YXRlWydzYXZlcG9pbnRQYXRoJ11cclxuXHJcbiAgICAgIGlmICRzY29wZS5zdGF0ZVsnYWxsb3dOb25SZXN0b3JlZFN0YXRlJ11cclxuICAgICAgICByZXF1ZXN0WydhbGxvd05vblJlc3RvcmVkU3RhdGUnXSA9ICRzY29wZS5zdGF0ZVsnYWxsb3dOb25SZXN0b3JlZFN0YXRlJ11cclxuICAgICAgICBxdWVyeVBhcmFtZXRlcnNbJ2FsbG93Tm9uUmVzdG9yZWRTdGF0ZSddID0gJHNjb3BlLnN0YXRlWydhbGxvd05vblJlc3RvcmVkU3RhdGUnXVxyXG5cclxuICAgICAgSm9iU3VibWl0U2VydmljZS5ydW5Kb2IoXHJcbiAgICAgICAgJHNjb3BlLnN0YXRlLnNlbGVjdGVkLCByZXF1ZXN0LCBxdWVyeVBhcmFtZXRlcnNcclxuICAgICAgKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAgIGlmIGFjdGlvbiA9PSAkc2NvcGUuc3RhdGVbJ2FjdGlvbi10aW1lJ11cclxuICAgICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIlxyXG4gICAgICAgICAgaWYgZGF0YS5lcnJvcj9cclxuICAgICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvclxyXG4gICAgICAgICAgZWxzZSBpZiBkYXRhLmVycm9ycz9cclxuICAgICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvcnNbMF1cclxuICAgICAgICAgIGlmIGRhdGEuam9iaWQ/XHJcbiAgICAgICAgICAgICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLCB7am9iaWQ6IGRhdGEuam9iaWR9KVxyXG4gICAgICAuY2F0Y2ggKGVycikgLT5cclxuICAgICAgICAkc2NvcGUuc3RhdGVbJ3N1Ym1pdC1idXR0b24nXSA9IFwiU3VibWl0XCJcclxuICAgICAgICAkc2NvcGUuZXJyb3IgPSBlcnJcclxuXHJcbiAgIyBqb2IgcGxhbiBkaXNwbGF5IHJlbGF0ZWQgc3R1ZmZcclxuICAkc2NvcGUubm9kZWlkID0gbnVsbFxyXG4gICRzY29wZS5jaGFuZ2VOb2RlID0gKG5vZGVpZCkgLT5cclxuICAgIGlmIG5vZGVpZCAhPSAkc2NvcGUubm9kZWlkXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBub2RlaWRcclxuICAgICAgJHNjb3BlLnZlcnRleCA9IG51bGxcclxuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbFxyXG4gICAgICAkc2NvcGUuYWNjdW11bGF0b3JzID0gbnVsbFxyXG5cclxuICAgICAgJHNjb3BlLiRicm9hZGNhc3QgJ3JlbG9hZCdcclxuXHJcbiAgICBlbHNlXHJcbiAgICAgICRzY29wZS5ub2RlaWQgPSBudWxsXHJcbiAgICAgICRzY29wZS5ub2RlVW5mb2xkZWQgPSBmYWxzZVxyXG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbFxyXG4gICAgICAkc2NvcGUuc3VidGFza3MgPSBudWxsXHJcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsXHJcblxyXG4gICRzY29wZS5jbGVhckZpbGVzID0gKCkgLT5cclxuICAgICRzY29wZS51cGxvYWRlciA9IHt9XHJcblxyXG4gICRzY29wZS51cGxvYWRGaWxlcyA9IChmaWxlcykgLT5cclxuICAgICMgbWFrZSBzdXJlIGV2ZXJ5dGhpbmcgaXMgY2xlYXIgYWdhaW4uXHJcbiAgICAkc2NvcGUudXBsb2FkZXIgPSB7fVxyXG4gICAgaWYgZmlsZXMubGVuZ3RoID09IDFcclxuICAgICAgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10gPSBmaWxlc1swXVxyXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ3VwbG9hZCddID0gdHJ1ZVxyXG4gICAgZWxzZVxyXG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSBcIkRpZCB5YSBmb3JnZXQgdG8gc2VsZWN0IGEgZmlsZT9cIlxyXG5cclxuICAkc2NvcGUuc3RhcnRVcGxvYWQgPSAoKSAtPlxyXG4gICAgaWYgJHNjb3BlLnVwbG9hZGVyWydmaWxlJ10/XHJcbiAgICAgIGZvcm1kYXRhID0gbmV3IEZvcm1EYXRhKClcclxuICAgICAgZm9ybWRhdGEuYXBwZW5kKFwiamFyZmlsZVwiLCAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSlcclxuICAgICAgJHNjb3BlLnVwbG9hZGVyWyd1cGxvYWQnXSA9IGZhbHNlXHJcbiAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJJbml0aWFsaXppbmcgdXBsb2FkLi4uXCJcclxuICAgICAgeGhyID0gbmV3IFhNTEh0dHBSZXF1ZXN0KClcclxuICAgICAgeGhyLnVwbG9hZC5vbnByb2dyZXNzID0gKGV2ZW50KSAtPlxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbFxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IHBhcnNlSW50KDEwMCAqIGV2ZW50LmxvYWRlZCAvIGV2ZW50LnRvdGFsKVxyXG4gICAgICB4aHIudXBsb2FkLm9uZXJyb3IgPSAoZXZlbnQpIC0+XHJcbiAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydwcm9ncmVzcyddID0gbnVsbFxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IFwiQW4gZXJyb3Igb2NjdXJyZWQgd2hpbGUgdXBsb2FkaW5nIHlvdXIgZmlsZVwiXHJcbiAgICAgIHhoci51cGxvYWQub25sb2FkID0gKGV2ZW50KSAtPlxyXG4gICAgICAgICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IG51bGxcclxuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiU2F2aW5nLi4uXCJcclxuICAgICAgeGhyLm9ucmVhZHlzdGF0ZWNoYW5nZSA9ICgpIC0+XHJcbiAgICAgICAgaWYgeGhyLnJlYWR5U3RhdGUgPT0gNFxyXG4gICAgICAgICAgcmVzcG9uc2UgPSBKU09OLnBhcnNlKHhoci5yZXNwb25zZVRleHQpXHJcbiAgICAgICAgICBpZiByZXNwb25zZS5lcnJvcj9cclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gcmVzcG9uc2UuZXJyb3JcclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsXHJcbiAgICAgICAgICBlbHNlIGlmIHJlc3BvbnNlLmVycm9ycz9cclxuICAgICAgICAgICAgJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gcmVzcG9uc2UuZXJyb3JzWzBdXHJcbiAgICAgICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbFxyXG4gICAgICAgICAgZWxzZVxyXG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3N1Y2Nlc3MnXSA9IFwiVXBsb2FkZWQhXCJcclxuICAgICAgeGhyLm9wZW4oXCJQT1NUXCIsIGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiamFycy91cGxvYWRcIilcclxuICAgICAgeGhyLnNlbmQoZm9ybWRhdGEpXHJcbiAgICBlbHNlXHJcbiAgICAgIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKVxyXG5cclxuLmZpbHRlciAnZ2V0SmFyU2VsZWN0Q2xhc3MnLCAtPlxyXG4gIChzZWxlY3RlZCwgYWN0dWFsKSAtPlxyXG4gICAgaWYgc2VsZWN0ZWQgPT0gYWN0dWFsXHJcbiAgICAgIFwiZmEtY2hlY2stc3F1YXJlXCJcclxuICAgIGVsc2VcclxuICAgICAgXCJmYS1zcXVhcmUtb1wiXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLmNvbnRyb2xsZXIoJ0pvYlN1Ym1pdENvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIEpvYlN1Ym1pdFNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcsICRzdGF0ZSwgJGxvY2F0aW9uKSB7XG4gIHZhciByZWZyZXNoO1xuICAkc2NvcGUueWFybiA9ICRsb2NhdGlvbi5hYnNVcmwoKS5pbmRleE9mKFwiL3Byb3h5L2FwcGxpY2F0aW9uX1wiKSAhPT0gLTE7XG4gICRzY29wZS5sb2FkTGlzdCA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiBKb2JTdWJtaXRTZXJ2aWNlLmxvYWRKYXJMaXN0KCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICAkc2NvcGUuYWRkcmVzcyA9IGRhdGEuYWRkcmVzcztcbiAgICAgIGlmIChkYXRhLmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgJHNjb3BlLm5vYWNjZXNzID0gZGF0YS5lcnJvcjtcbiAgICAgIH0gZWxzZSBpZiAoZGF0YS5lcnJvcnMgIT0gbnVsbCkge1xuICAgICAgICAkc2NvcGUubm9hY2Nlc3MgPSBkYXRhLmVycm9yc1swXTtcbiAgICAgIH1cbiAgICAgIHJldHVybiAkc2NvcGUuamFycyA9IGRhdGEuZmlsZXM7XG4gICAgfSk7XG4gIH07XG4gICRzY29wZS5kZWZhdWx0U3RhdGUgPSBmdW5jdGlvbigpIHtcbiAgICAkc2NvcGUucGxhbiA9IG51bGw7XG4gICAgJHNjb3BlLmVycm9yID0gbnVsbDtcbiAgICByZXR1cm4gJHNjb3BlLnN0YXRlID0ge1xuICAgICAgc2VsZWN0ZWQ6IG51bGwsXG4gICAgICBwYXJhbGxlbGlzbTogXCJcIixcbiAgICAgIHNhdmVwb2ludFBhdGg6IFwiXCIsXG4gICAgICBhbGxvd05vblJlc3RvcmVkU3RhdGU6IGZhbHNlLFxuICAgICAgJ2VudHJ5LWNsYXNzJzogXCJcIixcbiAgICAgICdwcm9ncmFtLWFyZ3MnOiBcIlwiLFxuICAgICAgJ3BsYW4tYnV0dG9uJzogXCJTaG93IFBsYW5cIixcbiAgICAgICdzdWJtaXQtYnV0dG9uJzogXCJTdWJtaXRcIixcbiAgICAgICdhY3Rpb24tdGltZSc6IDBcbiAgICB9O1xuICB9O1xuICAkc2NvcGUuZGVmYXVsdFN0YXRlKCk7XG4gICRzY29wZS51cGxvYWRlciA9IHt9O1xuICAkc2NvcGUubG9hZExpc3QoKTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gJHNjb3BlLmxvYWRMaXN0KCk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xuICAkc2NvcGUuc2VsZWN0SmFyID0gZnVuY3Rpb24oaWQpIHtcbiAgICBpZiAoJHNjb3BlLnN0YXRlLnNlbGVjdGVkID09PSBpZCkge1xuICAgICAgcmV0dXJuICRzY29wZS5kZWZhdWx0U3RhdGUoKTtcbiAgICB9IGVsc2Uge1xuICAgICAgJHNjb3BlLmRlZmF1bHRTdGF0ZSgpO1xuICAgICAgcmV0dXJuICRzY29wZS5zdGF0ZS5zZWxlY3RlZCA9IGlkO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLmRlbGV0ZUphciA9IGZ1bmN0aW9uKGV2ZW50LCBpZCkge1xuICAgIGlmICgkc2NvcGUuc3RhdGUuc2VsZWN0ZWQgPT09IGlkKSB7XG4gICAgICAkc2NvcGUuZGVmYXVsdFN0YXRlKCk7XG4gICAgfVxuICAgIGFuZ3VsYXIuZWxlbWVudChldmVudC5jdXJyZW50VGFyZ2V0KS5yZW1vdmVDbGFzcyhcImZhLXJlbW92ZVwiKS5hZGRDbGFzcyhcImZhLXNwaW4gZmEtc3Bpbm5lclwiKTtcbiAgICByZXR1cm4gSm9iU3VibWl0U2VydmljZS5kZWxldGVKYXIoaWQpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgYW5ndWxhci5lbGVtZW50KGV2ZW50LmN1cnJlbnRUYXJnZXQpLnJlbW92ZUNsYXNzKFwiZmEtc3BpbiBmYS1zcGlubmVyXCIpLmFkZENsYXNzKFwiZmEtcmVtb3ZlXCIpO1xuICAgICAgaWYgKGRhdGEuZXJyb3IgIT0gbnVsbCkge1xuICAgICAgICByZXR1cm4gYWxlcnQoZGF0YS5lcnJvcik7XG4gICAgICB9IGVsc2UgaWYgKGRhdGEuZXJyb3JzICE9IG51bGwpIHtcbiAgICAgICAgcmV0dXJuIGFsZXJ0KGRhdGEuZXJyb3JzWzBdKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgfTtcbiAgJHNjb3BlLmxvYWRFbnRyeUNsYXNzID0gZnVuY3Rpb24obmFtZSkge1xuICAgIHJldHVybiAkc2NvcGUuc3RhdGVbJ2VudHJ5LWNsYXNzJ10gPSBuYW1lO1xuICB9O1xuICAkc2NvcGUuZ2V0UGxhbiA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBhY3Rpb24sIHF1ZXJ5UGFyYW1ldGVycztcbiAgICBpZiAoJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID09PSBcIlNob3cgUGxhblwiKSB7XG4gICAgICBhY3Rpb24gPSBuZXcgRGF0ZSgpLmdldFRpbWUoKTtcbiAgICAgICRzY29wZS5zdGF0ZVsnYWN0aW9uLXRpbWUnXSA9IGFjdGlvbjtcbiAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiR2V0dGluZyBQbGFuXCI7XG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsO1xuICAgICAgJHNjb3BlLnBsYW4gPSBudWxsO1xuICAgICAgcXVlcnlQYXJhbWV0ZXJzID0ge307XG4gICAgICBpZiAoJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddKSB7XG4gICAgICAgIHF1ZXJ5UGFyYW1ldGVyc1snZW50cnktY2xhc3MnXSA9ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXTtcbiAgICAgIH1cbiAgICAgIGlmICgkc2NvcGUuc3RhdGUucGFyYWxsZWxpc20pIHtcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydwYXJhbGxlbGlzbSddID0gJHNjb3BlLnN0YXRlWydwYXJhbGxlbGlzbSddO1xuICAgICAgfVxuICAgICAgaWYgKCRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ10pIHtcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydwcm9ncmFtLWFyZ3MnXSA9ICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ107XG4gICAgICB9XG4gICAgICByZXR1cm4gSm9iU3VibWl0U2VydmljZS5nZXRQbGFuKCRzY29wZS5zdGF0ZS5zZWxlY3RlZCwgcXVlcnlQYXJhbWV0ZXJzKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgaWYgKGFjdGlvbiA9PT0gJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddKSB7XG4gICAgICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIjtcbiAgICAgICAgICBpZiAoZGF0YS5lcnJvciAhPSBudWxsKSB7XG4gICAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yO1xuICAgICAgICAgIH0gZWxzZSBpZiAoZGF0YS5lcnJvcnMgIT0gbnVsbCkge1xuICAgICAgICAgICAgJHNjb3BlLmVycm9yID0gZGF0YS5lcnJvcnNbMF07XG4gICAgICAgICAgfVxuICAgICAgICAgIHJldHVybiAkc2NvcGUucGxhbiA9IGRhdGEucGxhbjtcbiAgICAgICAgfVxuICAgICAgfSlbXCJjYXRjaFwiXShmdW5jdGlvbihlcnIpIHtcbiAgICAgICAgJHNjb3BlLnN0YXRlWydwbGFuLWJ1dHRvbiddID0gXCJTaG93IFBsYW5cIjtcbiAgICAgICAgcmV0dXJuICRzY29wZS5lcnJvciA9IGVycjtcbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLnJ1bkpvYiA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBhY3Rpb24sIHF1ZXJ5UGFyYW1ldGVycywgcmVxdWVzdDtcbiAgICBpZiAoJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPT09IFwiU3VibWl0XCIpIHtcbiAgICAgIGFjdGlvbiA9IG5ldyBEYXRlKCkuZ2V0VGltZSgpO1xuICAgICAgJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddID0gYWN0aW9uO1xuICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdHRpbmdcIjtcbiAgICAgICRzY29wZS5zdGF0ZVsncGxhbi1idXR0b24nXSA9IFwiU2hvdyBQbGFuXCI7XG4gICAgICAkc2NvcGUuZXJyb3IgPSBudWxsO1xuICAgICAgcmVxdWVzdCA9IHt9O1xuICAgICAgcXVlcnlQYXJhbWV0ZXJzID0ge307XG4gICAgICBpZiAoJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddKSB7XG4gICAgICAgIHJlcXVlc3RbJ2VudHJ5Q2xhc3MnXSA9ICRzY29wZS5zdGF0ZVsnZW50cnktY2xhc3MnXTtcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydlbnRyeS1jbGFzcyddID0gJHNjb3BlLnN0YXRlWydlbnRyeS1jbGFzcyddO1xuICAgICAgfVxuICAgICAgaWYgKCRzY29wZS5zdGF0ZS5wYXJhbGxlbGlzbSkge1xuICAgICAgICByZXF1ZXN0WydwYXJhbGxlbGlzbSddID0gJHNjb3BlLnN0YXRlWydwYXJhbGxlbGlzbSddO1xuICAgICAgICBxdWVyeVBhcmFtZXRlcnNbJ3BhcmFsbGVsaXNtJ10gPSAkc2NvcGUuc3RhdGVbJ3BhcmFsbGVsaXNtJ107XG4gICAgICB9XG4gICAgICBpZiAoJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXSkge1xuICAgICAgICByZXF1ZXN0Wydwcm9ncmFtQXJncyddID0gJHNjb3BlLnN0YXRlWydwcm9ncmFtLWFyZ3MnXTtcbiAgICAgICAgcXVlcnlQYXJhbWV0ZXJzWydwcm9ncmFtLWFyZ3MnXSA9ICRzY29wZS5zdGF0ZVsncHJvZ3JhbS1hcmdzJ107XG4gICAgICB9XG4gICAgICBpZiAoJHNjb3BlLnN0YXRlWydzYXZlcG9pbnRQYXRoJ10pIHtcbiAgICAgICAgcmVxdWVzdFsnc2F2ZXBvaW50UGF0aCddID0gJHNjb3BlLnN0YXRlWydzYXZlcG9pbnRQYXRoJ107XG4gICAgICAgIHF1ZXJ5UGFyYW1ldGVyc1snc2F2ZXBvaW50UGF0aCddID0gJHNjb3BlLnN0YXRlWydzYXZlcG9pbnRQYXRoJ107XG4gICAgICB9XG4gICAgICBpZiAoJHNjb3BlLnN0YXRlWydhbGxvd05vblJlc3RvcmVkU3RhdGUnXSkge1xuICAgICAgICByZXF1ZXN0WydhbGxvd05vblJlc3RvcmVkU3RhdGUnXSA9ICRzY29wZS5zdGF0ZVsnYWxsb3dOb25SZXN0b3JlZFN0YXRlJ107XG4gICAgICAgIHF1ZXJ5UGFyYW1ldGVyc1snYWxsb3dOb25SZXN0b3JlZFN0YXRlJ10gPSAkc2NvcGUuc3RhdGVbJ2FsbG93Tm9uUmVzdG9yZWRTdGF0ZSddO1xuICAgICAgfVxuICAgICAgcmV0dXJuIEpvYlN1Ym1pdFNlcnZpY2UucnVuSm9iKCRzY29wZS5zdGF0ZS5zZWxlY3RlZCwgcmVxdWVzdCwgcXVlcnlQYXJhbWV0ZXJzKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgaWYgKGFjdGlvbiA9PT0gJHNjb3BlLnN0YXRlWydhY3Rpb24tdGltZSddKSB7XG4gICAgICAgICAgJHNjb3BlLnN0YXRlWydzdWJtaXQtYnV0dG9uJ10gPSBcIlN1Ym1pdFwiO1xuICAgICAgICAgIGlmIChkYXRhLmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgICAgICRzY29wZS5lcnJvciA9IGRhdGEuZXJyb3I7XG4gICAgICAgICAgfSBlbHNlIGlmIChkYXRhLmVycm9ycyAhPSBudWxsKSB7XG4gICAgICAgICAgICAkc2NvcGUuZXJyb3IgPSBkYXRhLmVycm9yc1swXTtcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKGRhdGEuam9iaWQgIT0gbnVsbCkge1xuICAgICAgICAgICAgcmV0dXJuICRzdGF0ZS5nbyhcInNpbmdsZS1qb2IucGxhbi5zdWJ0YXNrc1wiLCB7XG4gICAgICAgICAgICAgIGpvYmlkOiBkYXRhLmpvYmlkXG4gICAgICAgICAgICB9KTtcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH0pW1wiY2F0Y2hcIl0oZnVuY3Rpb24oZXJyKSB7XG4gICAgICAgICRzY29wZS5zdGF0ZVsnc3VibWl0LWJ1dHRvbiddID0gXCJTdWJtaXRcIjtcbiAgICAgICAgcmV0dXJuICRzY29wZS5lcnJvciA9IGVycjtcbiAgICAgIH0pO1xuICAgIH1cbiAgfTtcbiAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICRzY29wZS5jaGFuZ2VOb2RlID0gZnVuY3Rpb24obm9kZWlkKSB7XG4gICAgaWYgKG5vZGVpZCAhPT0gJHNjb3BlLm5vZGVpZCkge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG5vZGVpZDtcbiAgICAgICRzY29wZS52ZXJ0ZXggPSBudWxsO1xuICAgICAgJHNjb3BlLnN1YnRhc2tzID0gbnVsbDtcbiAgICAgICRzY29wZS5hY2N1bXVsYXRvcnMgPSBudWxsO1xuICAgICAgcmV0dXJuICRzY29wZS4kYnJvYWRjYXN0KCdyZWxvYWQnKTtcbiAgICB9IGVsc2Uge1xuICAgICAgJHNjb3BlLm5vZGVpZCA9IG51bGw7XG4gICAgICAkc2NvcGUubm9kZVVuZm9sZGVkID0gZmFsc2U7XG4gICAgICAkc2NvcGUudmVydGV4ID0gbnVsbDtcbiAgICAgICRzY29wZS5zdWJ0YXNrcyA9IG51bGw7XG4gICAgICByZXR1cm4gJHNjb3BlLmFjY3VtdWxhdG9ycyA9IG51bGw7XG4gICAgfVxuICB9O1xuICAkc2NvcGUuY2xlYXJGaWxlcyA9IGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkc2NvcGUudXBsb2FkZXIgPSB7fTtcbiAgfTtcbiAgJHNjb3BlLnVwbG9hZEZpbGVzID0gZnVuY3Rpb24oZmlsZXMpIHtcbiAgICAkc2NvcGUudXBsb2FkZXIgPSB7fTtcbiAgICBpZiAoZmlsZXMubGVuZ3RoID09PSAxKSB7XG4gICAgICAkc2NvcGUudXBsb2FkZXJbJ2ZpbGUnXSA9IGZpbGVzWzBdO1xuICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsndXBsb2FkJ10gPSB0cnVlO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gXCJEaWQgeWEgZm9yZ2V0IHRvIHNlbGVjdCBhIGZpbGU/XCI7XG4gICAgfVxuICB9O1xuICByZXR1cm4gJHNjb3BlLnN0YXJ0VXBsb2FkID0gZnVuY3Rpb24oKSB7XG4gICAgdmFyIGZvcm1kYXRhLCB4aHI7XG4gICAgaWYgKCRzY29wZS51cGxvYWRlclsnZmlsZSddICE9IG51bGwpIHtcbiAgICAgIGZvcm1kYXRhID0gbmV3IEZvcm1EYXRhKCk7XG4gICAgICBmb3JtZGF0YS5hcHBlbmQoXCJqYXJmaWxlXCIsICRzY29wZS51cGxvYWRlclsnZmlsZSddKTtcbiAgICAgICRzY29wZS51cGxvYWRlclsndXBsb2FkJ10gPSBmYWxzZTtcbiAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gXCJJbml0aWFsaXppbmcgdXBsb2FkLi4uXCI7XG4gICAgICB4aHIgPSBuZXcgWE1MSHR0cFJlcXVlc3QoKTtcbiAgICAgIHhoci51cGxvYWQub25wcm9ncmVzcyA9IGZ1bmN0aW9uKGV2ZW50KSB7XG4gICAgICAgICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbDtcbiAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsncHJvZ3Jlc3MnXSA9IHBhcnNlSW50KDEwMCAqIGV2ZW50LmxvYWRlZCAvIGV2ZW50LnRvdGFsKTtcbiAgICAgIH07XG4gICAgICB4aHIudXBsb2FkLm9uZXJyb3IgPSBmdW5jdGlvbihldmVudCkge1xuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBudWxsO1xuICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydlcnJvciddID0gXCJBbiBlcnJvciBvY2N1cnJlZCB3aGlsZSB1cGxvYWRpbmcgeW91ciBmaWxlXCI7XG4gICAgICB9O1xuICAgICAgeGhyLnVwbG9hZC5vbmxvYWQgPSBmdW5jdGlvbihldmVudCkge1xuICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ3Byb2dyZXNzJ10gPSBudWxsO1xuICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIlNhdmluZy4uLlwiO1xuICAgICAgfTtcbiAgICAgIHhoci5vbnJlYWR5c3RhdGVjaGFuZ2UgPSBmdW5jdGlvbigpIHtcbiAgICAgICAgdmFyIHJlc3BvbnNlO1xuICAgICAgICBpZiAoeGhyLnJlYWR5U3RhdGUgPT09IDQpIHtcbiAgICAgICAgICByZXNwb25zZSA9IEpTT04ucGFyc2UoeGhyLnJlc3BvbnNlVGV4dCk7XG4gICAgICAgICAgaWYgKHJlc3BvbnNlLmVycm9yICE9IG51bGwpIHtcbiAgICAgICAgICAgICRzY29wZS51cGxvYWRlclsnZXJyb3InXSA9IHJlc3BvbnNlLmVycm9yO1xuICAgICAgICAgICAgcmV0dXJuICRzY29wZS51cGxvYWRlclsnc3VjY2VzcyddID0gbnVsbDtcbiAgICAgICAgICB9IGVsc2UgaWYgKHJlc3BvbnNlLmVycm9ycyAhPSBudWxsKSB7XG4gICAgICAgICAgICAkc2NvcGUudXBsb2FkZXJbJ2Vycm9yJ10gPSByZXNwb25zZS5lcnJvcnNbMF07XG4gICAgICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBudWxsO1xuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICByZXR1cm4gJHNjb3BlLnVwbG9hZGVyWydzdWNjZXNzJ10gPSBcIlVwbG9hZGVkIVwiO1xuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfTtcbiAgICAgIHhoci5vcGVuKFwiUE9TVFwiLCBmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvdXBsb2FkXCIpO1xuICAgICAgcmV0dXJuIHhoci5zZW5kKGZvcm1kYXRhKTtcbiAgICB9IGVsc2Uge1xuICAgICAgcmV0dXJuIGNvbnNvbGUubG9nKFwiVW5leHBlY3RlZCBFcnJvci4gVGhpcyBzaG91bGQgbm90IGhhcHBlblwiKTtcbiAgICB9XG4gIH07XG59KS5maWx0ZXIoJ2dldEphclNlbGVjdENsYXNzJywgZnVuY3Rpb24oKSB7XG4gIHJldHVybiBmdW5jdGlvbihzZWxlY3RlZCwgYWN0dWFsKSB7XG4gICAgaWYgKHNlbGVjdGVkID09PSBhY3R1YWwpIHtcbiAgICAgIHJldHVybiBcImZhLWNoZWNrLXNxdWFyZVwiO1xuICAgIH0gZWxzZSB7XG4gICAgICByZXR1cm4gXCJmYS1zcXVhcmUtb1wiO1xuICAgIH1cbiAgfTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnSm9iU3VibWl0U2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxyXG5cclxuICBAbG9hZEphckxpc3QgPSAoKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiamFycy9cIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGRlbGV0ZUphciA9IChpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmRlbGV0ZShmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGdldFBsYW4gPSAoaWQsIGFyZ3MpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqYXJzL1wiICsgZW5jb2RlVVJJQ29tcG9uZW50KGlkKSArIFwiL3BsYW5cIiwge3BhcmFtczogYXJnc30pXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YSlcclxuICAgIC5lcnJvciAoZXJyKSAtPlxyXG4gICAgICBpZiBlcnIuZXJyb3JzP1xyXG4gICAgICAgIGRlZmVycmVkLnJlamVjdChlcnIuZXJyb3JzWzBdKVxyXG4gICAgICBlbHNlXHJcbiAgICAgICAgZGVmZXJyZWQucmVqZWN0KGVycilcclxuXHJcbiAgICBkZWZlcnJlZC5wcm9taXNlXHJcblxyXG4gIEBydW5Kb2IgPSAoaWQsIHJlcXVlc3QsIHF1ZXJ5UGFyYW1ldGVycykgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLnBvc3QoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJqYXJzL1wiICsgZW5jb2RlVVJJQ29tcG9uZW50KGlkKSArIFwiL3J1blwiLCByZXF1ZXN0LCB7cGFyYW1zOiBxdWVyeVBhcmFtZXRlcnN9KVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcbiAgICAuZXJyb3IgKGVycikgLT5cclxuICAgICAgaWYgZXJyLmVycm9ycz9cclxuICAgICAgICBkZWZlcnJlZC5yZWplY3QoZXJyLmVycm9yc1swXSlcclxuICAgICAgZWxzZVxyXG4gICAgICAgIGRlZmVycmVkLnJlamVjdChlcnIpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ0pvYlN1Ym1pdFNlcnZpY2UnLCBmdW5jdGlvbigkaHR0cCwgZmxpbmtDb25maWcsICRxKSB7XG4gIHRoaXMubG9hZEphckxpc3QgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmRlbGV0ZUphciA9IGZ1bmN0aW9uKGlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cFtcImRlbGV0ZVwiXShmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5nZXRQbGFuID0gZnVuY3Rpb24oaWQsIGFyZ3MpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcImphcnMvXCIgKyBlbmNvZGVVUklDb21wb25lbnQoaWQpICsgXCIvcGxhblwiLCB7XG4gICAgICBwYXJhbXM6IGFyZ3NcbiAgICB9KS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KS5lcnJvcihmdW5jdGlvbihlcnIpIHtcbiAgICAgIGlmIChlcnIuZXJyb3JzICE9IG51bGwpIHtcbiAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlamVjdChlcnIuZXJyb3JzWzBdKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZWplY3QoZXJyKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgdGhpcy5ydW5Kb2IgPSBmdW5jdGlvbihpZCwgcmVxdWVzdCwgcXVlcnlQYXJhbWV0ZXJzKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5wb3N0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwiamFycy9cIiArIGVuY29kZVVSSUNvbXBvbmVudChpZCkgKyBcIi9ydW5cIiwgcmVxdWVzdCwge1xuICAgICAgcGFyYW1zOiBxdWVyeVBhcmFtZXRlcnNcbiAgICB9KS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhKTtcbiAgICB9KS5lcnJvcihmdW5jdGlvbihlcnIpIHtcbiAgICAgIGlmIChlcnIuZXJyb3JzICE9IG51bGwpIHtcbiAgICAgICAgcmV0dXJuIGRlZmVycmVkLnJlamVjdChlcnIuZXJyb3JzWzBdKTtcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHJldHVybiBkZWZlcnJlZC5yZWplY3QoZXJyKTtcbiAgICAgIH1cbiAgICB9KTtcbiAgICByZXR1cm4gZGVmZXJyZWQucHJvbWlzZTtcbiAgfTtcbiAgcmV0dXJuIHRoaXM7XG59KTtcbiIsIiNcclxuIyBMaWNlbnNlZCB0byB0aGUgQXBhY2hlIFNvZnR3YXJlIEZvdW5kYXRpb24gKEFTRikgdW5kZXIgb25lXHJcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXHJcbiMgZGlzdHJpYnV0ZWQgd2l0aCB0aGlzIHdvcmsgZm9yIGFkZGl0aW9uYWwgaW5mb3JtYXRpb25cclxuIyByZWdhcmRpbmcgY29weXJpZ2h0IG93bmVyc2hpcC4gIFRoZSBBU0YgbGljZW5zZXMgdGhpcyBmaWxlXHJcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxyXG4jIFwiTGljZW5zZVwiKTsgeW91IG1heSBub3QgdXNlIHRoaXMgZmlsZSBleGNlcHQgaW4gY29tcGxpYW5jZVxyXG4jIHdpdGggdGhlIExpY2Vuc2UuICBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcclxuI1xyXG4jICAgICBodHRwOi8vd3d3LmFwYWNoZS5vcmcvbGljZW5zZXMvTElDRU5TRS0yLjBcclxuI1xyXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcclxuIyBkaXN0cmlidXRlZCB1bmRlciB0aGUgTGljZW5zZSBpcyBkaXN0cmlidXRlZCBvbiBhbiBcIkFTIElTXCIgQkFTSVMsXHJcbiMgV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXHJcbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxyXG4jIGxpbWl0YXRpb25zIHVuZGVyIHRoZSBMaWNlbnNlLlxyXG4jXHJcblxyXG5hbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKVxyXG5cclxuLmNvbnRyb2xsZXIgJ092ZXJ2aWV3Q29udHJvbGxlcicsICgkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIC0+XHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyID0gLT5cclxuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKVxyXG4gICAgJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJylcclxuXHJcbiAgSm9ic1NlcnZpY2UucmVnaXN0ZXJPYnNlcnZlcigkc2NvcGUuam9iT2JzZXJ2ZXIpXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcilcclxuXHJcbiAgJHNjb3BlLmpvYk9ic2VydmVyKClcclxuXHJcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4gKGRhdGEpIC0+XHJcbiAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXHJcblxyXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cclxuICAgIE92ZXJ2aWV3U2VydmljZS5sb2FkT3ZlcnZpZXcoKS50aGVuIChkYXRhKSAtPlxyXG4gICAgICAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhXHJcbiAgLCBmbGlua0NvbmZpZ1tcInJlZnJlc2gtaW50ZXJ2YWxcIl1cclxuXHJcbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxyXG4gICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxyXG4iLCJhbmd1bGFyLm1vZHVsZSgnZmxpbmtBcHAnKS5jb250cm9sbGVyKCdPdmVydmlld0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsIE92ZXJ2aWV3U2VydmljZSwgSm9ic1NlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHJlZnJlc2g7XG4gICRzY29wZS5qb2JPYnNlcnZlciA9IGZ1bmN0aW9uKCkge1xuICAgICRzY29wZS5ydW5uaW5nSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ3J1bm5pbmcnKTtcbiAgICByZXR1cm4gJHNjb3BlLmZpbmlzaGVkSm9icyA9IEpvYnNTZXJ2aWNlLmdldEpvYnMoJ2ZpbmlzaGVkJyk7XG4gIH07XG4gIEpvYnNTZXJ2aWNlLnJlZ2lzdGVyT2JzZXJ2ZXIoJHNjb3BlLmpvYk9ic2VydmVyKTtcbiAgJHNjb3BlLiRvbignJGRlc3Ryb3knLCBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gSm9ic1NlcnZpY2UudW5SZWdpc3Rlck9ic2VydmVyKCRzY29wZS5qb2JPYnNlcnZlcik7XG4gIH0pO1xuICAkc2NvcGUuam9iT2JzZXJ2ZXIoKTtcbiAgT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgIHJldHVybiAkc2NvcGUub3ZlcnZpZXcgPSBkYXRhO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gT3ZlcnZpZXdTZXJ2aWNlLmxvYWRPdmVydmlldygpLnRoZW4oZnVuY3Rpb24oZGF0YSkge1xuICAgICAgcmV0dXJuICRzY29wZS5vdmVydmlldyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnT3ZlcnZpZXdTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgb3ZlcnZpZXcgPSB7fVxyXG5cclxuICBAbG9hZE92ZXJ2aWV3ID0gLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcIm92ZXJ2aWV3XCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIG92ZXJ2aWV3ID0gZGF0YVxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcbiIsImFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpLnNlcnZpY2UoJ092ZXJ2aWV3U2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdmFyIG92ZXJ2aWV3O1xuICBvdmVydmlldyA9IHt9O1xuICB0aGlzLmxvYWRPdmVydmlldyA9IGZ1bmN0aW9uKCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwib3ZlcnZpZXdcIikuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgb3ZlcnZpZXcgPSBkYXRhO1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSk7XG4iLCIjXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcbiMgb3IgbW9yZSBjb250cmlidXRvciBsaWNlbnNlIGFncmVlbWVudHMuICBTZWUgdGhlIE5PVElDRSBmaWxlXG4jIGRpc3RyaWJ1dGVkIHdpdGggdGhpcyB3b3JrIGZvciBhZGRpdGlvbmFsIGluZm9ybWF0aW9uXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcbiMgdG8geW91IHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZVxuIyBcIkxpY2Vuc2VcIik7IHlvdSBtYXkgbm90IHVzZSB0aGlzIGZpbGUgZXhjZXB0IGluIGNvbXBsaWFuY2VcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxuI1xuIyAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4jXG4jIFVubGVzcyByZXF1aXJlZCBieSBhcHBsaWNhYmxlIGxhdyBvciBhZ3JlZWQgdG8gaW4gd3JpdGluZywgc29mdHdhcmVcbiMgZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cbiMgU2VlIHRoZSBMaWNlbnNlIGZvciB0aGUgc3BlY2lmaWMgbGFuZ3VhZ2UgZ292ZXJuaW5nIHBlcm1pc3Npb25zIGFuZFxuIyBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiNcblxuYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJylcblxuLmNvbnRyb2xsZXIgJ0FsbFRhc2tNYW5hZ2Vyc0NvbnRyb2xsZXInLCAoJHNjb3BlLCBUYXNrTWFuYWdlcnNTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLm1hbmFnZXJzID0gZGF0YVxuXG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwgLT5cbiAgICBUYXNrTWFuYWdlcnNTZXJ2aWNlLmxvYWRNYW5hZ2VycygpLnRoZW4gKGRhdGEpIC0+XG4gICAgICAkc2NvcGUubWFuYWdlcnMgPSBkYXRhXG4gICwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdXG5cbiAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaClcblxuLmNvbnRyb2xsZXIgJ1NpbmdsZVRhc2tNYW5hZ2VyQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUubWV0cmljcyA9IHt9XG4gIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkTWV0cmljcygkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5tZXRyaWNzID0gZGF0YVxuXG4gICAgcmVmcmVzaCA9ICRpbnRlcnZhbCAtPlxuICAgICAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgICAkc2NvcGUubWV0cmljcyA9IGRhdGFcbiAgICAsIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXVxuXG4gICAgJHNjb3BlLiRvbiAnJGRlc3Ryb3knLCAtPlxuICAgICAgJGludGVydmFsLmNhbmNlbChyZWZyZXNoKVxuXG4uY29udHJvbGxlciAnU2luZ2xlVGFza01hbmFnZXJMb2dzQ29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUubG9nID0ge31cbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQgPSAkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZFxuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZExvZ3MoJHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWQpLnRoZW4gKGRhdGEpIC0+XG4gICAgJHNjb3BlLmxvZyA9IGRhdGFcblxuICAkc2NvcGUucmVsb2FkRGF0YSA9ICgpIC0+XG4gICAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRMb2dzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuIChkYXRhKSAtPlxuICAgICAgJHNjb3BlLmxvZyA9IGRhdGFcblxuLmNvbnRyb2xsZXIgJ1NpbmdsZVRhc2tNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcicsICgkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSAtPlxuICAkc2NvcGUuc3Rkb3V0ID0ge31cbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQgPSAkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZFxuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZFN0ZG91dCgkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAkc2NvcGUuc3Rkb3V0ID0gZGF0YVxuXG4gICRzY29wZS5yZWxvYWREYXRhID0gKCkgLT5cbiAgICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZFN0ZG91dCgkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbiAoZGF0YSkgLT5cbiAgICAgICRzY29wZS5zdGRvdXQgPSBkYXRhXG5cbi5jb250cm9sbGVyICdTaW5nbGVUYXNrTWFuYWdlckFydGhhc0NvbnRyb2xsZXInLCgkc2NvcGUsJHN0YXRlUGFyYW1zLFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZSwkaW50ZXJ2YWwsZmxpbmtDb25maWcpLT5cbiAgJHNjb3BlLnRlc3RhYT1cImhlbGxvXCJcbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQ9JHN0YXRlUGFyYW1zLnRhc2ttYW5hZ2VyaWRcbiAgeHRlcm09e31cbiAgd3M9e31cbiAgJHNjb3BlLmluaXQ9KCktPlxuICAgIHBhdGg9J3dzOi8vMTI3LjAuMC4xOjM2NTgvd3MnXG4gICAgd3M9IG5ldyBXZWJTb2NrZXQgcGF0aFxuICAgIGNvbnNvbGUubG9nKHdzKVxuICAgIHdzLm9uZXJyb3I9KCktPlxuICAgICAgd3MuY2xvc2UoKVxuICAgICAgd3M9bnVsbFxuICAgIHdzLm9ub3Blbj0oKS0+XG4gICAgICB4dGVybT1uZXcgVGVybWluYWwge2NvbHM6MTcwLHJvd3M6NTAsc2NyZWVuUmVhZGVyTW9kZTp0cnVlLHJlbmRlcmVyVHlwZTpcImNhbnZhc1wiLGNvbnZlcnRFb2w6dHJ1ZX1cbiAgICAgIHdzLm9ubWVzc2FnZT0oZXZlbnQpLT5cbiAgICAgICAgaWYgZXZlbnQudHlwZT09J21lc3NhZ2UnXG4gICAgICAgICAgeHRlcm0ud3JpdGUoZXZlbnQuZGF0YSlcbiAgICAgIHh0ZXJtLm9wZW4oZG9jdW1lbnQuZ2V0RWxlbWVudEJ5SWQoJ3Rlcm1pbmFsJykpXG4gICAgICB4dGVybS5vbiAnZGF0YScsIChkYXRhKS0+XG4gICAgICAgIHdzLnNlbmQoSlNPTi5zdHJpbmdpZnkoe2FjdGlvbjogJ3JlYWQnLCBkYXRhOiBkYXRhfSkpXG4gICAgICB3cy5zZW5kKEpTT04uc3RyaW5naWZ5KHthY3Rpb246ICdyZXNpemUnLCBjb2xzOiAxNzAsIHJvd3M6IDUwfSkpXG4gICRzY29wZS5pbml0KClcblxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuY29udHJvbGxlcignQWxsVGFza01hbmFnZXJzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgVGFza01hbmFnZXJzU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgVGFza01hbmFnZXJzU2VydmljZS5sb2FkTWFuYWdlcnMoKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm1hbmFnZXJzID0gZGF0YTtcbiAgfSk7XG4gIHJlZnJlc2ggPSAkaW50ZXJ2YWwoZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFRhc2tNYW5hZ2Vyc1NlcnZpY2UubG9hZE1hbmFnZXJzKCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgICByZXR1cm4gJHNjb3BlLm1hbmFnZXJzID0gZGF0YTtcbiAgICB9KTtcbiAgfSwgZmxpbmtDb25maWdbXCJyZWZyZXNoLWludGVydmFsXCJdKTtcbiAgcmV0dXJuICRzY29wZS4kb24oJyRkZXN0cm95JywgZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuICRpbnRlcnZhbC5jYW5jZWwocmVmcmVzaCk7XG4gIH0pO1xufSkuY29udHJvbGxlcignU2luZ2xlVGFza01hbmFnZXJDb250cm9sbGVyJywgZnVuY3Rpb24oJHNjb3BlLCAkc3RhdGVQYXJhbXMsIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZSwgJGludGVydmFsLCBmbGlua0NvbmZpZykge1xuICB2YXIgcmVmcmVzaDtcbiAgJHNjb3BlLm1ldHJpY3MgPSB7fTtcbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLm1ldHJpY3MgPSBkYXRhO1xuICB9KTtcbiAgcmVmcmVzaCA9ICRpbnRlcnZhbChmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRNZXRyaWNzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUubWV0cmljcyA9IGRhdGE7XG4gICAgfSk7XG4gIH0sIGZsaW5rQ29uZmlnW1wicmVmcmVzaC1pbnRlcnZhbFwiXSk7XG4gIHJldHVybiAkc2NvcGUuJG9uKCckZGVzdHJveScsIGZ1bmN0aW9uKCkge1xuICAgIHJldHVybiAkaW50ZXJ2YWwuY2FuY2VsKHJlZnJlc2gpO1xuICB9KTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyTG9nc0NvbnRyb2xsZXInLCBmdW5jdGlvbigkc2NvcGUsICRzdGF0ZVBhcmFtcywgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLCAkaW50ZXJ2YWwsIGZsaW5rQ29uZmlnKSB7XG4gICRzY29wZS5sb2cgPSB7fTtcbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQgPSAkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZDtcbiAgU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRMb2dzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICByZXR1cm4gJHNjb3BlLmxvZyA9IGRhdGE7XG4gIH0pO1xuICByZXR1cm4gJHNjb3BlLnJlbG9hZERhdGEgPSBmdW5jdGlvbigpIHtcbiAgICByZXR1cm4gU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlLmxvYWRMb2dzKCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUubG9nID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyU3Rkb3V0Q29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgJHNjb3BlLnN0ZG91dCA9IHt9O1xuICAkc2NvcGUudGFza21hbmFnZXJpZCA9ICRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkO1xuICBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UubG9hZFN0ZG91dCgkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZCkudGhlbihmdW5jdGlvbihkYXRhKSB7XG4gICAgcmV0dXJuICRzY29wZS5zdGRvdXQgPSBkYXRhO1xuICB9KTtcbiAgcmV0dXJuICRzY29wZS5yZWxvYWREYXRhID0gZnVuY3Rpb24oKSB7XG4gICAgcmV0dXJuIFNpbmdsZVRhc2tNYW5hZ2VyU2VydmljZS5sb2FkU3Rkb3V0KCRzdGF0ZVBhcmFtcy50YXNrbWFuYWdlcmlkKS50aGVuKGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgIHJldHVybiAkc2NvcGUuc3Rkb3V0ID0gZGF0YTtcbiAgICB9KTtcbiAgfTtcbn0pLmNvbnRyb2xsZXIoJ1NpbmdsZVRhc2tNYW5hZ2VyQXJ0aGFzQ29udHJvbGxlcicsIGZ1bmN0aW9uKCRzY29wZSwgJHN0YXRlUGFyYW1zLCBTaW5nbGVUYXNrTWFuYWdlclNlcnZpY2UsICRpbnRlcnZhbCwgZmxpbmtDb25maWcpIHtcbiAgdmFyIHdzLCB4dGVybTtcbiAgJHNjb3BlLnRlc3RhYSA9IFwiaGVsbG9cIjtcbiAgJHNjb3BlLnRhc2ttYW5hZ2VyaWQgPSAkc3RhdGVQYXJhbXMudGFza21hbmFnZXJpZDtcbiAgeHRlcm0gPSB7fTtcbiAgd3MgPSB7fTtcbiAgJHNjb3BlLmluaXQgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgcGF0aDtcbiAgICBwYXRoID0gJ3dzOi8vMTI3LjAuMC4xOjM2NTgvd3MnO1xuICAgIHdzID0gbmV3IFdlYlNvY2tldChwYXRoKTtcbiAgICBjb25zb2xlLmxvZyh3cyk7XG4gICAgd3Mub25lcnJvciA9IGZ1bmN0aW9uKCkge1xuICAgICAgd3MuY2xvc2UoKTtcbiAgICAgIHJldHVybiB3cyA9IG51bGw7XG4gICAgfTtcbiAgICByZXR1cm4gd3Mub25vcGVuID0gZnVuY3Rpb24oKSB7XG4gICAgICB4dGVybSA9IG5ldyBUZXJtaW5hbCh7XG4gICAgICAgIGNvbHM6IDE3MCxcbiAgICAgICAgcm93czogNTAsXG4gICAgICAgIHNjcmVlblJlYWRlck1vZGU6IHRydWUsXG4gICAgICAgIHJlbmRlcmVyVHlwZTogXCJjYW52YXNcIixcbiAgICAgICAgY29udmVydEVvbDogdHJ1ZVxuICAgICAgfSk7XG4gICAgICB3cy5vbm1lc3NhZ2UgPSBmdW5jdGlvbihldmVudCkge1xuICAgICAgICBpZiAoZXZlbnQudHlwZSA9PT0gJ21lc3NhZ2UnKSB7XG4gICAgICAgICAgcmV0dXJuIHh0ZXJtLndyaXRlKGV2ZW50LmRhdGEpO1xuICAgICAgICB9XG4gICAgICB9O1xuICAgICAgeHRlcm0ub3Blbihkb2N1bWVudC5nZXRFbGVtZW50QnlJZCgndGVybWluYWwnKSk7XG4gICAgICB4dGVybS5vbignZGF0YScsIGZ1bmN0aW9uKGRhdGEpIHtcbiAgICAgICAgcmV0dXJuIHdzLnNlbmQoSlNPTi5zdHJpbmdpZnkoe1xuICAgICAgICAgIGFjdGlvbjogJ3JlYWQnLFxuICAgICAgICAgIGRhdGE6IGRhdGFcbiAgICAgICAgfSkpO1xuICAgICAgfSk7XG4gICAgICByZXR1cm4gd3Muc2VuZChKU09OLnN0cmluZ2lmeSh7XG4gICAgICAgIGFjdGlvbjogJ3Jlc2l6ZScsXG4gICAgICAgIGNvbHM6IDE3MCxcbiAgICAgICAgcm93czogNTBcbiAgICAgIH0pKTtcbiAgICB9O1xuICB9O1xuICByZXR1cm4gJHNjb3BlLmluaXQoKTtcbn0pO1xuIiwiI1xyXG4jIExpY2Vuc2VkIHRvIHRoZSBBcGFjaGUgU29mdHdhcmUgRm91bmRhdGlvbiAoQVNGKSB1bmRlciBvbmVcclxuIyBvciBtb3JlIGNvbnRyaWJ1dG9yIGxpY2Vuc2UgYWdyZWVtZW50cy4gIFNlZSB0aGUgTk9USUNFIGZpbGVcclxuIyBkaXN0cmlidXRlZCB3aXRoIHRoaXMgd29yayBmb3IgYWRkaXRpb25hbCBpbmZvcm1hdGlvblxyXG4jIHJlZ2FyZGluZyBjb3B5cmlnaHQgb3duZXJzaGlwLiAgVGhlIEFTRiBsaWNlbnNlcyB0aGlzIGZpbGVcclxuIyB0byB5b3UgdW5kZXIgdGhlIEFwYWNoZSBMaWNlbnNlLCBWZXJzaW9uIDIuMCAodGhlXHJcbiMgXCJMaWNlbnNlXCIpOyB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlXHJcbiMgd2l0aCB0aGUgTGljZW5zZS4gIFlvdSBtYXkgb2J0YWluIGEgY29weSBvZiB0aGUgTGljZW5zZSBhdFxyXG4jXHJcbiMgICAgIGh0dHA6Ly93d3cuYXBhY2hlLm9yZy9saWNlbnNlcy9MSUNFTlNFLTIuMFxyXG4jXHJcbiMgVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxyXG4jIGRpc3RyaWJ1dGVkIHVuZGVyIHRoZSBMaWNlbnNlIGlzIGRpc3RyaWJ1dGVkIG9uIGFuIFwiQVMgSVNcIiBCQVNJUyxcclxuIyBXSVRIT1VUIFdBUlJBTlRJRVMgT1IgQ09ORElUSU9OUyBPRiBBTlkgS0lORCwgZWl0aGVyIGV4cHJlc3Mgb3IgaW1wbGllZC5cclxuIyBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXHJcbiMgbGltaXRhdGlvbnMgdW5kZXIgdGhlIExpY2Vuc2UuXHJcbiNcclxuXHJcbmFuZ3VsYXIubW9kdWxlKCdmbGlua0FwcCcpXHJcblxyXG4uc2VydmljZSAnVGFza01hbmFnZXJzU2VydmljZScsICgkaHR0cCwgZmxpbmtDb25maWcsICRxKSAtPlxyXG4gIEBsb2FkTWFuYWdlcnMgPSAoKSAtPlxyXG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpXHJcblxyXG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzXCIpXHJcbiAgICAuc3VjY2VzcyAoZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIC0+XHJcbiAgICAgIGRlZmVycmVkLnJlc29sdmUoZGF0YVsndGFza21hbmFnZXJzJ10pXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAXHJcblxyXG4uc2VydmljZSAnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIC0+XHJcbiAgQGxvYWRNZXRyaWNzID0gKHRhc2ttYW5hZ2VyaWQpIC0+XHJcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKClcclxuXHJcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkKVxyXG4gICAgLnN1Y2Nlc3MgKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSAtPlxyXG4gICAgICBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpXHJcblxyXG4gICAgZGVmZXJyZWQucHJvbWlzZVxyXG5cclxuICBAbG9hZExvZ3MgPSAodGFza21hbmFnZXJpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQgKyBcIi9sb2dcIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQGxvYWRTdGRvdXQgPSAodGFza21hbmFnZXJpZCkgLT5cclxuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKVxyXG5cclxuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vycy9cIiArIHRhc2ttYW5hZ2VyaWQgKyBcIi9zdGRvdXRcIilcclxuICAgIC5zdWNjZXNzIChkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykgLT5cclxuICAgICAgZGVmZXJyZWQucmVzb2x2ZShkYXRhKVxyXG5cclxuICAgIGRlZmVycmVkLnByb21pc2VcclxuXHJcbiAgQFxyXG5cclxuIiwiYW5ndWxhci5tb2R1bGUoJ2ZsaW5rQXBwJykuc2VydmljZSgnVGFza01hbmFnZXJzU2VydmljZScsIGZ1bmN0aW9uKCRodHRwLCBmbGlua0NvbmZpZywgJHEpIHtcbiAgdGhpcy5sb2FkTWFuYWdlcnMgPSBmdW5jdGlvbigpIHtcbiAgICB2YXIgZGVmZXJyZWQ7XG4gICAgZGVmZXJyZWQgPSAkcS5kZWZlcigpO1xuICAgICRodHRwLmdldChmbGlua0NvbmZpZy5qb2JTZXJ2ZXIgKyBcInRhc2ttYW5hZ2Vyc1wiKS5zdWNjZXNzKGZ1bmN0aW9uKGRhdGEsIHN0YXR1cywgaGVhZGVycywgY29uZmlnKSB7XG4gICAgICByZXR1cm4gZGVmZXJyZWQucmVzb2x2ZShkYXRhWyd0YXNrbWFuYWdlcnMnXSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHJldHVybiB0aGlzO1xufSkuc2VydmljZSgnU2luZ2xlVGFza01hbmFnZXJTZXJ2aWNlJywgZnVuY3Rpb24oJGh0dHAsIGZsaW5rQ29uZmlnLCAkcSkge1xuICB0aGlzLmxvYWRNZXRyaWNzID0gZnVuY3Rpb24odGFza21hbmFnZXJpZCkge1xuICAgIHZhciBkZWZlcnJlZDtcbiAgICBkZWZlcnJlZCA9ICRxLmRlZmVyKCk7XG4gICAgJGh0dHAuZ2V0KGZsaW5rQ29uZmlnLmpvYlNlcnZlciArIFwidGFza21hbmFnZXJzL1wiICsgdGFza21hbmFnZXJpZCkuc3VjY2VzcyhmdW5jdGlvbihkYXRhLCBzdGF0dXMsIGhlYWRlcnMsIGNvbmZpZykge1xuICAgICAgcmV0dXJuIGRlZmVycmVkLnJlc29sdmUoZGF0YSk7XG4gICAgfSk7XG4gICAgcmV0dXJuIGRlZmVycmVkLnByb21pc2U7XG4gIH07XG4gIHRoaXMubG9hZExvZ3MgPSBmdW5jdGlvbih0YXNrbWFuYWdlcmlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkICsgXCIvbG9nXCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICB0aGlzLmxvYWRTdGRvdXQgPSBmdW5jdGlvbih0YXNrbWFuYWdlcmlkKSB7XG4gICAgdmFyIGRlZmVycmVkO1xuICAgIGRlZmVycmVkID0gJHEuZGVmZXIoKTtcbiAgICAkaHR0cC5nZXQoZmxpbmtDb25maWcuam9iU2VydmVyICsgXCJ0YXNrbWFuYWdlcnMvXCIgKyB0YXNrbWFuYWdlcmlkICsgXCIvc3Rkb3V0XCIpLnN1Y2Nlc3MoZnVuY3Rpb24oZGF0YSwgc3RhdHVzLCBoZWFkZXJzLCBjb25maWcpIHtcbiAgICAgIHJldHVybiBkZWZlcnJlZC5yZXNvbHZlKGRhdGEpO1xuICAgIH0pO1xuICAgIHJldHVybiBkZWZlcnJlZC5wcm9taXNlO1xuICB9O1xuICByZXR1cm4gdGhpcztcbn0pO1xuIl19
