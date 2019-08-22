

## 说明
### 代码说明
- 克隆代码之后，新建自己的分支，更新代码，然后推送到GIT
- 线下机器 spslave28.bigdata.ly
- 启动集群之后master地址: http://spslave28.bigdata.ly:8081

### 官方地址
- https://tianchi.aliyun.com/competition/entrance/231742/information
- https://github.com/flink-tpc-ds

## 代码部分
### 克隆代码
```
git clone git@github.com:tongcheng-elong/flink.git
git checkout -b test-master origin/tpcds-master
```

### 提交代码
```
git add -A
git commit -am "update"
git push origin test-master:test-master
```

## 集群操作

### 更新代码
```
cd /home/hadoop/flink
git fetch
git checkout test-master
git pull --rebase
```

### 编译
```
cd /home/hadoop/flink
mvn clean install -DskipTests=true -Dfast -T 2C -Dmaven.compile.fork=true
```

### 添加Parquet支持
```
cd /home/hadoop/flink/flink-dist/target/flink-1.9-tpcds-master-bin/flink-1.9-tpcds-master
cp /work/JAVA_WORK/mvn_repo/repos/org/apache/parquet/parquet-column/1.8.2/parquet-column-1.8.2.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/apache/parquet/parquet-hadoop/1.8.2/parquet-hadoop-1.8.2.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/apache/parquet/parquet-common/1.8.2/parquet-common-1.8.2.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/apache/parquet/parquet-encoding/1.8.2/parquet-encoding-1.8.2.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/apache/parquet/parquet-format/2.3.1/parquet-format-2.3.1.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/codehaus/jackson/jackson-core-asl/1.8.8/jackson-core-asl-1.8.8.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/codehaus/jackson/jackson-mapper-asl/1.8.8/jackson-mapper-asl-1.8.8.jar lib/  
cp /work/JAVA_WORK/mvn_repo/repos/org/apache/flink/flink-shaded-hadoop-2-uber/2.4.1-7.0/flink-shaded-hadoop-2-uber-2.4.1-7.0.jar lib/  
```

### 启动集群
```
cd /home/hadoop/flink/flink-dist/target/flink-1.9-tpcds-master-bin/flink-1.9-tpcds-master
rm -rf log/*
jps | grep StandaloneSessionClusterEntrypoint | kill -9 `awk '{print $1}'`
jps | grep TaskManager | kill -9 `awk '{print $1}'`
bin/jobmanager.sh start
bin/taskmanager.sh start
```

### 测试1G数据
```
/home/hadoop/flink/flink-dist/target/flink-1.9-tpcds-master-bin/flink-1.9-tpcds-master/bin/flink \
run -c com.alibaba.flink.benchmark.perf.QueryBenchmark \
/home/hadoop/flink-community-perf/target/flink-community-perf-1.0-SNAPSHOT-jar-with-dependencies.jar \
--scaleFactor 1 --dataLocation /home/hadoop/flink-community-perf/resource/tpcds/datagen/data/SF1 \
--sqlLocation /home/hadoop/flink-community-perf/resource/tpcds/querygen/queries/SF100 \
--sqlQueries all \
--numIters 1 \
--sqlType tpcds \
--sourceType parquet \
--optimizedPlanCollect true \
--dumpFileOfOptimizedPlan /tmp/plan/tpcds \
--operatorMetricCollect false \
--dumpFileOfPlanWithMetrics /tmp/metrics \
--analyzeTable true
```


### 测试100G数据
```
/home/hadoop/flink/flink-dist/target/flink-1.9-tpcds-master-bin/flink-1.9-tpcds-master/bin/flink \
run -c com.alibaba.flink.benchmark.perf.QueryBenchmark \
/home/hadoop/flink-community-perf/target/flink-community-perf-1.0-SNAPSHOT-jar-with-dependencies.jar \
--scaleFactor 100 --dataLocation /home/hadoop/flink-community-perf/resource/tpcds/datagen/data/SF100 \
--sqlLocation /home/hadoop/flink-community-perf/resource/tpcds/querygen/queries/SF100 \
--sqlQueries all \
--numIters 1 \
--sqlType tpcds \
--sourceType csv \
--optimizedPlanCollect true \
--dumpFileOfOptimizedPlan /tmp/plan/tpcds \
--operatorMetricCollect false \
--dumpFileOfPlanWithMetrics /tmp/metrics \
--analyzeTable true
```






















