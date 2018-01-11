package example.VC;

import example.SG.SimpleTest;
import example.data.RandomWalkSimRankData;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/10/26
 * @Time: 下午9:55
 * @Project: GellyPractice
 */
public class RandomWalkSimRank<VV,EV> implements GraphAlgorithm<Long,VV,EV,DataSet<Tuple3<Long,Long,Double>>> {


    private int maxIterations;

    private static  double c = 0.85;

    private  static  double threshold = 0.00;

    public RandomWalkSimRank(int maxIterations,double threshold_) {
        this.maxIterations = maxIterations;
        threshold = threshold_;
    }

    @Override
    public DataSet<Tuple3<Long,Long,Double>> run(Graph<Long, VV, EV> input) throws Exception {


        Graph<Long,VertexValue,EV> graphProcessed = input.mapVertices(new ToCustomVertexValueMapper<VV>());

        DataSet<Vertex<Long,VertexValue>> list = graphProcessed.getVertices();
        List<Vertex<Long,VertexValue>> l = list.collect();
        for(Iterator<Vertex<Long,VertexValue>> iter = l.iterator(); iter.hasNext(); ){
            Vertex<Long,VertexValue> v = iter.next();
            System.out.println(v.getId() + "    " + v.getValue().toString());
        }

        ScatterGatherConfiguration configuration = new ScatterGatherConfiguration();

        configuration.setOptDegrees(true);

        Graph<Long,VertexValue,EV> graph = graphProcessed.runScatterGatherIteration(new SimRankScatterFunction<EV>(),new SimRankGatherFunction(),this.maxIterations,configuration);

        DataSet<Vertex<Long,VertexValue>> values = graph.getVertices();

        //

        List<Vertex<Long,VertexValue>> vlist = values.collect();

        for(Vertex<Long,VertexValue> v : vlist){
            System.out.println("vertex id : " + v.f0);
            System.out.println("VertexValue: " + v.f1.valueToString());
            System.out.println("VertexPaths: " + v.f1.pathsToString());
        }

        //

        DataSet<Tuple4<Long,Long,Long,Double>> paths = values.flatMap(new FlatMapFunction<Vertex<Long, VertexValue>, Tuple4<Long, Long, Long, Double>>() {
            @Override
            public void flatMap(Vertex<Long, VertexValue> value, Collector<Tuple4<Long, Long, Long, Double>> out) throws Exception {
//                List<Tuple4<Long,Long,Long,Double>> list = value.getValue().value;
//
//                for(Tuple4<Long,Long,Long,Double> t : list){
//                    out.collect(t);
//                }
                for(Tuple4<Long,Long,Long,Double> t : value.getValue().value){

                    Long f0 = Long.valueOf(t.f0.toString());
                    Long f1 =  Long.valueOf(t.f1.toString());
                    Long f2 = Long.valueOf(t.f2.toString());
                    Double f3 = t.f3;

                    Tuple4<Long,Long,Long,Double> nt = new Tuple4<>(f0,f1,f2,f3);
                    out.collect(nt);
                }
            }
        });

        //

        List<Tuple4<Long,Long,Long,Double>> list1 = paths.collect();

        for(Tuple4<Long,Long,Long,Double> t : list1){
            System.out.println(t.toString());
        }

        //

        DataSet<Tuple3<Long,Long,Double>> simPair =  paths.groupBy(0,1,2).reduce(new ReduceFunction<Tuple4<Long, Long, Long, Double>>() {
            @Override
            public Tuple4<Long, Long, Long, Double> reduce(Tuple4<Long, Long, Long, Double> value1, Tuple4<Long, Long, Long, Double> value2) throws Exception {
                return new Tuple4<Long,Long,Long,Double>(value1.f0,value1.f1,value1.f2,value1.f3 + value2.f3);
            }
        }).groupBy(1,2).combineGroup(new GroupCombineFunction<Tuple4<Long,Long,Long,Double>, Tuple3<Long,Long,Double>>() {
            @Override
            public void combine(Iterable<Tuple4<Long, Long, Long, Double>> values, Collector<Tuple3<Long,Long,Double>> out) throws Exception {

                List<Tuple4<Long,Long,Long,Double>> copy = new ArrayList<>();
                Iterator<Tuple4<Long,Long,Long,Double>> iter = values.iterator();
                for(;iter.hasNext();){
                    copy.add(iter.next());
                }
                int n = copy.size();

                for(int i = 0; i < n ; i++){
                    for(int j = i + 1; j < n ; j ++){
                        Tuple4<Long,Long,Long,Double> t1 = copy.get(i);
                        Tuple4<Long,Long,Long,Double> t2 = copy.get(j);
                        if(!t1.f0.equals(t2.f0)){
                            Tuple3<Long,Long,Double> tuple3 = new Tuple3<>(t1.f0,t2.f0,Double.valueOf(t1.f3) + Double.valueOf(t2.f3));
                            out.collect(tuple3);
                        }
                    }
                }
            }
        });


        //

        List<Tuple3<Long,Long,Double>> list2 = simPair.collect();

        for(Tuple3<Long,Long,Double> t : list2){
            System.out.println(t.toString());
        }

        //

        return simPair.groupBy(0,1)
                .reduce(new ReduceFunction<Tuple3<Long, Long, Double>>() {
                    @Override
                    public Tuple3<Long, Long, Double> reduce(Tuple3<Long, Long, Double> value1, Tuple3<Long, Long, Double> value2) throws Exception {
                        return new Tuple3<>(value1.f0,value1.f1,value1.f2 + value2.f2);
                    }
                });
    }



    public static final class SimRankScatterFunction<EV> extends ScatterFunction<Long,VertexValue,Message,EV>{

        private static double threshold = 0.00;

        @Override
        public void preSuperstep() throws Exception {
            super.preSuperstep();
            threshold = RandomWalkSimRank.threshold;
        }

        @Override
        public void sendMessages(Vertex<Long, VertexValue> vertex) throws Exception {

            if(getSuperstepNumber() == 1){
                Long n = getOutDegree();

                double  p = (1/(double)n) * c;

                Message msg = new Message();

                List<Long>  l = new ArrayList<Long>();

                l.add(vertex.getId());

                Tuple2<List<Long>,Double> tuple2 = new Tuple2<>(l,p);

                msg.list.add(tuple2);

                sendMessageToAllNeighbors(msg);

            }else{

                VertexValue vertexValue = vertex.getValue();

                Long  out = getOutDegree();

                Message  msg = new Message();

                List<Tuple2<List<Long>,Double>> list = vertexValue.paths;

                for(Iterator<Tuple2<List<Long>,Double>> iterator = list.iterator(); iterator.hasNext();){
                    Tuple2<List<Long>,Double> tuple2 = iterator.next();

                    Double p_pre = tuple2.f1;

                    double p_cur = p_pre * (1/(double)out) * c;

                    if(p_cur <  threshold) {
                        continue;
                    }

                    tuple2.setField(p_cur,1);

                    tuple2.f0.add(vertex.getId());

                    msg.list.add(tuple2);
                }

                sendMessageToAllNeighbors(msg);
            }

        }

    }


    public  static final class SimRankGatherFunction extends GatherFunction<Long,VertexValue,Message>{


        @Override
        public void updateVertex(Vertex<Long, VertexValue> vertex, org.apache.flink.graph.spargel.MessageIterator<Message> inMessages) throws Exception {

            ArrayList<Tuple2<List<Long>,Double>> paths  = new ArrayList<>();
            ArrayList<Tuple4<Long,Long,Long,Double>> value = vertex.getValue().value;



            while(inMessages.hasNext()){
                Message  message = inMessages.next();

                for(Iterator<Tuple2<List<Long>,Double>> iterator = message.list.iterator();iterator.hasNext();){

                    Tuple2<List<Long>,Double> tuple2 = iterator.next();
                    paths.add(tuple2);
                }
            }

            System.out.println("SuperStep: " + getSuperstepNumber() + "  vertexId: " + vertex.getId() + "current paths: ");
            for(Iterator<Tuple2<List<Long>,Double>> iterator = paths.iterator();iterator.hasNext();){
                Tuple2<List<Long>,Double> path = iterator.next();
                System.out.println("SuperStep: " + getSuperstepNumber() + "  vertexId: " + vertex.getId() + Arrays.deepToString(path.f0.toArray()) + "  p =" + path.f1);
            }



            for(Tuple2<List<Long>,Double> path : paths){
                List<Long> trace = path.f0;
                Double p = path.f1;
                long s = trace.size();
                System.out.println("SuperStep: " + getSuperstepNumber() + "  vertexId: "  + vertex.getId() + "path :  " + Arrays.deepToString(trace.toArray()) + " | size = " +  s);
                Long first = trace.get(0);
                Long last = trace.get((int)s - 1);
                if(first != last){
                    Tuple4<Long,Long,Long,Double> tuple4 = new Tuple4<>(first,s,last,p);
                    value.add(tuple4);
                }
            }

            VertexValue vertexValue = vertex.getValue();

            vertexValue.setValue(value);
            vertexValue.setPaths(paths);

            int k = getSuperstepNumber();

            System.out.println("SuperStep = " + getSuperstepNumber() + " Available memory = " + Runtime.getRuntime().freeMemory());

            setNewVertexValue(vertexValue);

        }

    }


    private static final class ToCustomVertexValueMapper<VV> implements MapFunction<Vertex<Long,VV>,VertexValue>{

        @Override
        public VertexValue map(Vertex<Long, VV> value) throws Exception {
            return new VertexValue(new ArrayList<Tuple4<Long,Long,Long,Double>>(),new ArrayList<Tuple2<List<Long>,Double>>());
        }
    }


    public static class Message{

        public List<Tuple2<List<Long>,Double>> list = new ArrayList<>();

        public Message() {
        }
    }



    public static  class VertexValue implements Serializable{

        private static final long serialVersionUID = 1L;

//         ("1_1_3",0.78)
        private ArrayList<Tuple4<Long,Long,Long,Double>> value;



        private ArrayList<Tuple2<List<Long>,Double>> paths;




        public VertexValue() {

        }

        public VertexValue(ArrayList<Tuple4<Long,Long,Long,Double>> value, ArrayList<Tuple2<List<Long>, Double>> paths) {
            this.value = value;
            this.paths = paths;
        }

        public ArrayList<Tuple4<Long,Long,Long,Double>> getValue() {
            return value;
        }

        public void setValue(ArrayList<Tuple4<Long,Long,Long,Double>> value) {
            this.value = value;
        }

        public ArrayList<Tuple2<List<Long>, Double>> getPaths() {
            return paths;
        }

        public void setPaths(ArrayList<Tuple2<List<Long>, Double>> paths) {
            this.paths = paths;
        }

        @Override
        public String toString() {
            return "VertexValue{" +
                    "value=" + Arrays.toString(value.toArray()) +
                    ", paths=" + Arrays.toString(paths.toArray()) +
                    '}';
        }


        public String valueToString(){

            StringBuilder result = new StringBuilder();
            result.append("value : ");
            for(Tuple4<Long,Long,Long,Double> t : value){
                result.append(t.toString()).append("\n");
            }

            return result.toString();
        }


        public String pathsToString(){

            String result = "paths : ";

            for(Tuple2<List<Long>,Double> t : paths){

                result += "[" + Arrays.deepToString(t.f0.toArray()) + "]" + " " + t.f1 + '\n';
            }

            return result;
        }


    }


    public static void main(String[] args)throws Exception{



        ParameterTool parameterTool  = ParameterTool.fromArgs(args);

        int maxIterations = parameterTool.getInt("maxIterations",20);

        double threshold = parameterTool.getDouble("threshold",0.0001);

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        System.out.println("Max memory : " +  Runtime.getRuntime().maxMemory());


        Graph<Long,List<Long>,NullValue> graph = Graph.fromDataSet
                (RandomWalkSimRankData.getDefaultVertexDataset(env),RandomWalkSimRankData.getDefaultEdgeDataset(env),env);


        DataSet<Tuple3<Long,Long,Double>>result = new RandomWalkSimRank<List<Long>,NullValue>(maxIterations,threshold).run(graph);

        result.print();

    }
}
