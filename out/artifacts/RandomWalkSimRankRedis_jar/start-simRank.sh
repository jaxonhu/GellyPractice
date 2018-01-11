#!/bin/bash

#--paths 10 --maxIterations 2 --threshold 0.001 --jedisServer localhost --redisPort 7000 --deploy local --graphDataPath /Users/jaxon/github/GraphTestDataset/polbooks_un_105_441.edges

graphData=simple_un_8_9.edges
graphDataPath=hdfs:///graph/input/${graphData}
outputPath=hdfs:///graph/output/SimRank
jedisServer=localhost
redisPort=7000
parallelism=8
jobname=SimRank
threshold=0.001
deploy=cluster
maxIterations=5
flinkPath=/Users/jaxon/tools/flink/flink-binarys/flink-1.3.2/bin 
jarPath=/Users/jaxon/code2/GellyPractice/out/artifacts/RandomWalkSimRankRedis_jar

for parallelism  in 1
do
${flinkPath}/flink  run -class example.VC.RandomWalkSimRankRedis  ${jarPath}/RandomWalkSimRankRedis.jar  --maxIterations ${maxIterations} --threshold ${threshold}   --jedisServer ${jedisServer}  --redisPort ${redisPort}  --parallelism ${parallelism}  --jobName  "${jobname}_${maxIterations}_${threshold}_${graphData}" --graphDataPath ${graphDataPath} --outputPath ${outputPath} --deploy ${deploy}
done
wait
