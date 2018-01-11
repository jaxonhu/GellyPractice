package example.VC;

import example.data.ModularityTestData;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/9/27
 * @Time: 下午7:56
 * @Project: GellyPractice
 */
public class SumAggregatorTest<K,EV> implements GraphAlgorithm<K,Long,EV,Integer> {


    private int maxIterations;



    public SumAggregatorTest(int maxIterations) {
        this.maxIterations = maxIterations;
    }


    @Override
    public Integer run(Graph<K, Long, EV> input) throws Exception {

//        VertexCentricConfiguration vcc = new VertexCentricConfiguration();
//
//        vcc.setName("LongSumAggregatorT");
//
//        vcc.registerAggregator("sumAggregator",new LongSumAggregator());

        input.runVertexCentricIteration(new MyComputeFunction<K,EV>(),null,this.maxIterations);

        return null;
    }



    public static class MyComputeFunction<K,EV> extends ComputeFunction<K,Long,EV,Long>{


        @Override
        public void preSuperstep() throws Exception {
            super.preSuperstep();
        }

        @Override
        public void compute(Vertex<K, Long> vertex, MessageIterator<Long> longs) throws Exception {


            if(getSuperstepNumber() == 1){

                sendMessageToAllNeighbors(vertex.getValue());

            }else if(getSuperstepNumber() == 2){

                long currentSum = 0;

                for(;longs.hasNext();){

                    currentSum += longs.next();

                }



                sendMessageToAllNeighbors(vertex.getValue());
            }else if(getSuperstepNumber() == 3){


            }

        }
    }

    public static void main(String[] args)throws  Exception{

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();





        Graph<Long,Long,Long> graph = new ModularityTestData().getDefaultGraph(env).getUndirected();


        new SumAggregatorTest<Long,Long>(10).run(graph);


    }

}
