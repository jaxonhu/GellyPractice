#!/bin/bash

# --srcId 1 --maxIterations 20 --deploy local --graphDataPath hdfs:///graph/input/simple_un_8_9.edges --outputPath hdfs:///graph/output --parallelism 2

graphDataPath=hdfs:///graph/input/simple_un_8_9.edges
outputPath=hdfs:///graph/output/GraphColoring
#dataSetName=dolphins_un_62_159.edges
dataSetName=simple_un_8_9.edges
srcId=1
colors=100
parallelism=1
jobname=GraphColoring
deploy=cluster
maxIterations=100

for parallelism  in 1
do
/Users/jaxon/tools/flink/flink-binarys/flink-1.3.2/bin/flink  run -class example.VC.GraphColoring /Users/jaxon/code2/GellyPractice/out/artifacts/GraphColoring_jar/GraphColoring.jar  --maxIterations ${maxIterations}    --parallelism ${parallelism}  --jobName  "${jobname}_${srcId}_${parallelism}_${dataSetName}" --graphDataPath ${graphDataPath} --outputPath ${outputPath} --deploy ${deploy}
done
wait
