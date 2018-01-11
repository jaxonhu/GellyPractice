package example.VC;
import example.data.RandomWalkSimRankData;
import example.utils.ReadFromText;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.*;
import org.apache.flink.graph.asm.degree.annotate.directed.VertexOutDegree;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/10/26
 * @Time: 下午9:55
 * @Project: GellyPractice
 */
public class  RandomWalkSimRankRedis<VV,EV> implements GraphAlgorithm<Long,VV,EV,DataSet<Vertex<Long,LongValue>>> {


    private int maxIterations;
    private static  double c = 0.85;
    private  double threshold;
    private  String redisServer;
    private  int redisPort;


    public static void main(String[] args)throws Exception{

        ParameterTool parameterTool  = ParameterTool.fromArgs(args);
        int maxIterations = parameterTool.getInt("maxIterations",20);
        double threshold = parameterTool.getDouble("threshold",0.001);
        int redisPort = parameterTool.getInt("redisPort",7000);
        int parallelism = parameterTool.getInt("parallelism",1);
        String redisServer = parameterTool.get("jedisServer","localhost");
        String graphDataPath = parameterTool.get("graphDataPath","");
        String deploy = parameterTool.get("deploy","local");
        String jobName = parameterTool.get("jobName","RandomWalkSimRank");
        String outputPath = parameterTool.get("outputPath");
        String statistic = parameterTool.get("statistic","false");

        // set execution environment
        ExecutionEnvironment env = null;
        if(deploy.equals("local")) {
            env = ExecutionEnvironment.createLocalEnvironment();
        }
        else {
            env = ExecutionEnvironment.getExecutionEnvironment();
        }

        env.setParallelism(parallelism);
        //read graph
        Graph<Long,List<Long>,NullValue> graph;

        if(graphDataPath.equals("")) {
            graph = Graph.fromDataSet
                    (RandomWalkSimRankData.getDefaultVertexDataset(env), RandomWalkSimRankData.getDefaultEdgeDataset(env), env);
        }
        else{
            DataSet<Edge<Long,NullValue>> edges = ReadFromText.getEdgesFromFile(graphDataPath,env);
            graph = Graph.fromDataSet(edges, new MapFunction<Long, List<Long>>(){
                @Override
                public List<Long> map(Long value) throws Exception {
                    return null;
                }
            }, env);

        }
        //execute RandomSimRankRedis algorithm
        DataSet<Vertex<Long,LongValue>> vertexDataSet = new RandomWalkSimRankRedis<List<Long>,NullValue>(maxIterations,threshold,redisServer,redisPort).run(graph);

        vertexDataSet.writeAsText(outputPath + "/" + jobName + "_" + System.currentTimeMillis());
        env.execute(jobName);

        // whether print the final rank results
        if("true".equals(statistic)) {
            List<Tuple2<String, Double>> resultFinal = RandomWalkSimRankRedis.getResultsFromRedis(redisServer, redisPort);

            String resultPath = "./" + jobName + "_results";
            PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(resultPath)));

            for (Tuple2<String, Double> t : resultFinal) {
                System.out.print(t.f0 + "     " + t.f1 + "\n");
                pw.print(t.f0 + "     " + t.f1 + "\n");
            }

            pw.close();
        }
    }

    private RandomWalkSimRankRedis(int maxIterations,double threshold_,String redisServer_,int redisPort_) {
        this.maxIterations = maxIterations;
        this.threshold = threshold_;
        this.redisServer = redisServer_;
        this.redisPort  = redisPort_;
    }

    @Override
    public DataSet<Vertex<Long,LongValue>> run(Graph<Long, VV, EV> input) throws Exception {

        Graph<Long,LongValue,EV> graphProcessed = input.mapVertices(new ToCustomVertexValueMapper<VV>());
        Graph<Long,LongValue,EV> graphAfterMap = graphProcessed.joinWithVertices(graphProcessed.outDegrees(),new LongValueVertexjoin());
        Graph<Long,LongValue,EV> graph = graphAfterMap.runVertexCentricIteration(new SimRankVC<EV>(this.redisServer,this.redisPort,this.threshold),null,this.maxIterations);
        return graph.getVertices();
    }

    private static List<Tuple2<String,Double>> getResultsFromRedis(String redisServer,int redisPort){

        List<Tuple2<String,Double>> results = new ArrayList<Tuple2<String, Double>>();
        Jedis jedis = null;
        try{
            jedis = new Jedis(redisServer,redisPort);
            Set<String> keys = jedis.smembers("vertex-pairs");
            for(String key : keys){
                Tuple2<String,Double> t = new Tuple2<>();
                Double value = Double.valueOf(jedis.hget(key,"value"));
                t.f0 = key;
                t.f1 = value;
                jedis.hdel(key,"value");
                results.add(t);
            }
            jedis.del("vertex-pairs");
        }finally {
            try{
                jedis.close();
            }catch (NullPointerException e){
                e.printStackTrace();
            }
        }

        //处理大图将耗时巨大，可以用最大堆或者快排解决之。
        Collections.sort(results, new Comparator<Tuple2<String, Double>>(){
            @Override
            public int compare(Tuple2<String, Double> o1, Tuple2<String, Double> o2) {
                if (o1.f1.doubleValue() == o2.f1.doubleValue()) {
                    return 0;
                }
                return o1.f1 > o2.f1 ?-1:1;
            }
        });


        return results;
    }


    public static final class LongValueVertexjoin implements VertexJoinFunction<LongValue,LongValue>{

        @Override
        public LongValue vertexJoin(LongValue vertexValue, LongValue inputValue) throws Exception {
            return new LongValue(inputValue.getValue());
        }
    }

    public static final class SimRankVC<EV> extends ComputeFunction<Long,LongValue,EV,Message> {

        private  double threshold;
        private  Jedis jedis;
        private  String redisServer;
        private  int redisPort;

        public SimRankVC(String redisServer, int redisPort, double threshold) {
            this.threshold = threshold;
            this.redisServer = redisServer;
            this.redisPort = redisPort;
        }

        @Override
        public void preSuperstep() throws Exception {
            super.preSuperstep();
            jedis = new Jedis(this.redisServer,this.redisPort);
        }

        @Override
        public void postSuperstep() throws Exception {
            super.postSuperstep();
            jedis.close();
        }

        @Override
        public void compute(Vertex<Long, LongValue> vertex, MessageIterator<Message> messages) throws Exception {

            if (getSuperstepNumber() == 1) {
                //calculate initial possibility
                long outDegree = vertex.getValue().getValue();
                long id = vertex.getId();
                double p = (1 / (double) outDegree) * c;
                Message msg = new Message();
                List<Long> l = new ArrayList<Long>();
                l.add(vertex.getId());
                Tuple2<List<Long>, Double> tuple2 = new Tuple2<>(l, p);
                msg.list.add(tuple2);
                // send message to neighbors
                Iterable<Edge<Long,EV>> edges =  getEdges();
                Iterator<Edge<Long,EV>> itor = edges.iterator();
                while (itor.hasNext()) {
                    Edge<Long, EV> e = itor.next();
                    long src = e.getSource();
                    long dst = e.getTarget();
                    if(src == id) {
                        sendMessageTo(e.getTarget(), msg);
                    }
                }
            } else {

                long outDegree = vertex.getValue().getValue();
                long id = vertex.getId();
                Message msgPost = new Message();
                msgPost.list = new ArrayList<Tuple2<List<Long>, Double>>();
                List<Message> msgList = new ArrayList<Message>();
                List<Tuple2<List<Long>,Double>> paths = new ArrayList<>();
                for (Message msg : messages) {
                    msgList.add(msg);
                }
                for(Message msg : msgList) {
                    paths.addAll(msg.list);
                }
                for (Message msg : msgList) {
                    List<Tuple2<List<Long>, Double>> tuple2s = msg.list;
                    for(Tuple2<List<Long>,Double> t : tuple2s){
                        List<Long> path = t.f0;
                        Double possibility = t.f1;
                        double p_cur = possibility * (1/(double)outDegree) * c;
                        if(p_cur < this.threshold){
                            continue;
                        }
                        Tuple2<List<Long>,Double> sendTuple2 = new Tuple2<>();
                        path.add(vertex.getId());
                        sendTuple2.f0 = path;
                        sendTuple2.f1 = p_cur;
                        msgPost.list.add(sendTuple2);
                    }
                }
                //calculate vertex pair's similarity
                for(int i = 0 ; i < paths.size() ; i++){
                    for(int j = i+1 ; j < paths.size() ; j++){
                        Tuple2<List<Long>,Double> path1 = paths.get(i);
                        Tuple2<List<Long>,Double> path2 = paths.get(j);
                        long src1 = path1.f0.get(0);
                        long src2 = path2.f0.get(0);
                        double p1 = path1.f1;
                        double p2 = path2.f1;
                        if(src1 == src2){
                            continue;
                        }
                        StringBuilder key = new StringBuilder();
                        if(src1 < src2){
                            key.append(src1).append("-").append(src2);
                        }else{
                            key.append(src2).append("-").append(src1);
                        }
                        jedis.sadd("vertex-pairs",key.toString());

                        double simP = 0.00;
                        if(outDegree != 0) {
                            simP = (p1 * (1 /(double)outDegree) * c + p2 * (1 / (double)outDegree) * c) / 2;
                        }
                        else{
                            simP = (p1 * c + p2 * c)/2;
                        }
                        String p_old = jedis.hget(key.toString(),"value");
                        Double p_old_d = 0.00;
                        if(p_old != null){
                            p_old_d = Double.valueOf(p_old);
                        }
                        double withDelta = p_old_d + simP;
                        jedis.hset(key.toString(),"value",withDelta + "");
                    }
                }
                //send message to out neighbors
                Iterable<Edge<Long,EV>> edges = getEdges();
                Iterator<Edge<Long, EV>> itor = edges.iterator();
                while (itor.hasNext()) {
                    Edge<Long, EV> e = itor.next();
                    long src = e.getSource();
                    long dst = e.getTarget();
                    if(src == id) {
                        sendMessageTo(dst, msgPost);
                    }
                }
            }

        }

    }



    private static final class ToCustomVertexValueMapper<VV> implements MapFunction<Vertex<Long,VV>,LongValue>{

        @Override
        public LongValue map(Vertex<Long, VV> value) throws Exception {

            return new LongValue(0);
        }
    }


    private  static class Message{

        private List<Tuple2<List<Long>,Double>> list = new ArrayList<>();

        private  Message() {}
    }



}
