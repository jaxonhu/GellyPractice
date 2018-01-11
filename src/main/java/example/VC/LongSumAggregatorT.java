package example.VC;

import example.data.ModularityTestData;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.graph.pregel.VertexCentricConfiguration;
import org.apache.flink.types.LongValue;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/9/27
 * @Time: 下午2:35
 * @Project: GellyPractice
 */
public class LongSumAggregatorT<K,EV> implements GraphAlgorithm<K,Long,EV,DataSet<Vertex<K,Long>>> {

    private int maxIterations;


    public LongSumAggregatorT(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    @Override
    public DataSet<Vertex<K,Long>> run(Graph<K, Long, EV> input) throws Exception {

        VertexCentricConfiguration vcc = new VertexCentricConfiguration();

        vcc.setName("LongSumAggregatorT");

        vcc.registerAggregator("sumAggregator",new LongSumAggregator());

        Graph<K,Long,EV> graph =  input.runVertexCentricIteration(new LSComputionFunction<K,EV>(),null,this.maxIterations,vcc);

        DataSet<Vertex<K,Long>> vertexDataSet = graph.getVertices();



        return vertexDataSet;

    }



    public static  class LSComputionFunction<K,EV> extends ComputeFunction<K,Long,EV,Long>{

        LongSumAggregator aggregator = new LongSumAggregator();



        public LSComputionFunction() {



        }

        @Override
        public void preSuperstep() throws Exception {
            //super.preSuperstep();

            aggregator = getIterationAggregator("sumAggregator");


        }

        @Override
        public void compute(Vertex<K, Long> vertex, MessageIterator<Long> longs) throws Exception {

            if(getSuperstepNumber() == 1){

                sendMessageToAllNeighbors(vertex.getValue());

            }else{

                long currentSum = 0;

                for(;longs.hasNext();){

                    currentSum += longs.next();
                    System.out.println("SuperStep = " + getSuperstepNumber() + " VertexId = " + vertex.getId() + "currentSum = " + currentSum);

                }

                aggregator.aggregate(currentSum);

                sendMessageToAllNeighbors(vertex.getValue());

            }

        }


        @Override
        public void postSuperstep() throws Exception {
            //super.postSuperstep();
            if(getSuperstepNumber() > 2){
                //System.out.println("helloworld SuperStep=" + getSuperstepNumber()+ "  " + ((LongValue)getPreviousIterationAggregate("sumAggregator")).getValue());
                System.out.println("helloworld SuperStep=" + getSuperstepNumber()+ "  " + aggregator.getAggregate().getValue());
            }
        }
    }



    public static void main(String[] args)throws  Exception{

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();





        Graph<Long,Long,Long> graph = new ModularityTestData().getDefaultGraph(env).getUndirected();


        DataSet<Vertex<Long,Long>> vertices =  new LongSumAggregatorT<Long,Long>(10).run(graph);

        vertices.print();

    }

}
