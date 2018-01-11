package example.VC;

import example.data.GraphColoringData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by jaxon on 2017/8/17.
 */
public class GraphColoring<K,EV> implements GraphAlgorithm<K, Long,EV,DataSet<Vertex<K,Long>>>{

    private static  Logger logger = LoggerFactory.getLogger(GraphColoring.class);

    private int colorNum;

    private  int  maxIterations;

    private  int srcId;

    /**
     * @Description: constructor
     * @Date: 2017/8/17
     * @param colorNum
     * @param maxIterations
     */
    public GraphColoring(int colorNum, Integer maxIterations, int srcId) {

        this.colorNum = colorNum;
        this.maxIterations = maxIterations;
        this.srcId = srcId;
    }

    @Override
    public DataSet<Vertex<K,Long>> run(Graph<K,Long,EV> input) throws Exception {

        return input.runVertexCentricIteration(new GraphColoringComputeFunction<K,EV>(this.colorNum,this.srcId),null,maxIterations)
                .getVertices();
    }

    public  static final class GraphColoringComputeFunction<K,EV> extends ComputeFunction<K, Long,EV,Long>{

        private int colorNum;

        private int srcId;

        public GraphColoringComputeFunction(int colorNum, int srcId) {
            this.colorNum = colorNum;
            this.srcId = srcId;
        }

        @Override
        public void compute(Vertex<K,Long> vertex, MessageIterator<Long> messageIterator) throws Exception {

            if(1 == getSuperstepNumber()){

                vertex.setValue(-1L);
                if( new Long(srcId).equals(vertex.getId())){
                    Random r = new Random(System.nanoTime());
                    int color = 1 + r.nextInt(this.colorNum+1);
                    vertex.setValue((long)color);
                    setNewVertexValue((long)color);
//                    System.out.println("SuperStep=1, VertexId = "+ vertex.getId() + " color = " + vertex.getValue());
                    logger.info("SuperStep=1, VertexId = "+ vertex.getId() + " color = " + vertex.getValue());
                    sendMessageToAllNeighbors(vertex.getValue());
                }

            }else{

                List<Long> neighbors = new ArrayList<>();
                Set<Long> nei_set = new HashSet<>();

                for(Long msg : messageIterator){
                    neighbors.add(msg);
                    nei_set.add(msg);
                }

                if(vertex.getValue() == -1){

                    List<Long> available = new ArrayList<>();
                    for(int j  = 1; j < colorNum + 1; j++) {
                        available.add((long) j);
                    }
                    for(int i = 1; i < neighbors.size()+1; i++) {
                        available.remove(neighbors.get(i - 1));
                    }
                    int size = available.size();

                    Random r = new Random(System.nanoTime());

                    Long colorthis = available.get(r.nextInt(size));

                    vertex.setValue(colorthis);
                    setNewVertexValue(colorthis);
//                    System.out.println("SuperStep = " + getSuperstepNumber() + "VertexId = " + vertex.getId() + "pick color = " + vertex.getValue());
                    logger.info("SuperStep = " + getSuperstepNumber() + "VertexId = " + vertex.getId() + "pick color = " + vertex.getValue());
                    sendMessageToAllNeighbors(colorthis);

                }else{
                    Long cur_color = vertex.getValue();
                    // conflict with neighbor's color
                    if(nei_set.contains(cur_color)){
                        List<Long> available = new ArrayList<>();
                        for(int i = 1 ; i < colorNum + 1; i++){
                            if(!nei_set.contains((long)i)) {
                                available.add((long) i);
                            }
                        }
                        int size = available.size();
                        Random r = new Random(System.nanoTime());
                        Long colorthis = available.get(r.nextInt(size));
                        vertex.setValue(colorthis);
                        setNewVertexValue(colorthis);
//                        System.out.println(" Conflict! SuperStep = " + getSuperstepNumber() + "VertexId = " + vertex.getId() + "pick color = " + vertex.getValue());
                        logger.info(" Conflict! SuperStep = " + getSuperstepNumber() + "VertexId = " + vertex.getId() + "pick color = " + vertex.getValue());
                        sendMessageToAllNeighbors(colorthis);
                    }
                }

            }

        }
    }

    public static void main(String[] args)throws Exception{

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String strSrcId = parameterTool.get("srcId","0");
        String strColorNum = parameterTool.get("colors");
        String strMaxIterations = parameterTool.get("maxIterations","100");
        String graphDataPath = parameterTool.get("graphDataPath","default");
        Integer parallelism = Integer.valueOf(parameterTool.get("parallelism"));
        String jobName = parameterTool.get("jobName","GraphColoring");
        String outputPath = parameterTool.get("outputPath","hdfs:///");
        String deploy = parameterTool.get("deploy","local");


        int srcId = Integer.parseInt(strSrcId);
        int colorNum = 0;
        if(parameterTool.has("colors")){
            colorNum = Integer.parseInt(strColorNum);
        }else{
            colorNum = 20;
        }

        int maxIterations = Integer.valueOf(strMaxIterations);

        ExecutionEnvironment env;
        if("local".equals(deploy)) {
            env = ExecutionEnvironment.createLocalEnvironment();
        }else{
            env = ExecutionEnvironment.getExecutionEnvironment();
        }
        Graph<Long, Long, NullValue> graph;

        if("default".equals(graphDataPath)){
             graph = Graph.fromDataSet(GraphColoringData.getDefaultVertexDatasets(env),
                    GraphColoringData.getDefaultEdgeDatasets(env), env).getUndirected();
        }else{
            DataSet<Edge<Long,NullValue>> edges =  GraphColoringData.getEdgeDataFromFile(graphDataPath,env);
            graph = Graph.fromDataSet(edges, new MapFunction<Long, Long>() {
                @Override
                public Long map(Long value) throws Exception {
                    return -1L;
                }
            },env).getUndirected();
        }

        long max = 0;
        if(!parameterTool.has("colors")){
//            DataSet<Vertex<Long,LongValue>> vertices =  new VertexDegree<Long,Long,NullValue>().run(graph);
            DataSet<Tuple2<Long,LongValue>> vertices = graph.getDegrees();
            List<Tuple2<Long,LongValue>> vertexList = vertices.collect();
            for(Tuple2<Long,LongValue> v : vertexList){
                if(v.f1.getValue() > max){
                    max = v.f1.getValue();
                }
            }
            colorNum = (int)max/2;
        }

        env.setParallelism(parallelism);

        DataSet<Vertex<Long,Long>> result = new GraphColoring<Long,NullValue>(colorNum,maxIterations,srcId).run(graph);
//        result.print();
//        graph.getEdges().print();
        result.writeAsText(outputPath + "/"+ jobName + "_" + System.currentTimeMillis());
        env.execute(jobName);

    }
}
