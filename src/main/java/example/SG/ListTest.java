package example.SG;

import example.data.ListTestData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

import java.lang.reflect.Array;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/8/31
 * @Time: 下午3:26
 * @Project: GellyPractice
 */
public class ListTest<K,EV> implements GraphAlgorithm<K,Tuple2<K,K[][]>,EV,DataSet<Vertex<K,Tuple2<K,K[][]>>>> {


    private static final int maxIterations = 10;

    private Class<K> type;

    @Override
    public DataSet<Vertex<K,Tuple2<K,K[][]>>> run(Graph<K, Tuple2<K,K[][]>, EV> input) throws Exception {

        Graph<K,Tuple2<K,K[][]>,EV> initialGraph = input.mapVertices(new MapFunction<Vertex<K,Tuple2<K,K[][]>>,Tuple2<K,K[][]>>() {
            @Override
            public Tuple2<K,K[][]> map(Vertex<K,Tuple2<K,K[][]>> vertex) throws Exception {

                //K[][] value = (K[][])Array.newInstance(type,10);
//                value[0][0] = vertex.getId();
//                Tuple2<K,K[][]> l = new Tuple2<>(vertex.getId(),value);
                return vertex.getValue();
            }
        });

        return  initialGraph.runVertexCentricIteration(new TestComputeFunction<K,EV>(),null,maxIterations).getVertices();

    }


    public static final class TestComputeFunction<K,EV> extends ComputeFunction<K,Tuple2<K,K[][]>,EV,Tuple2<K,K[][]>> {


        @Override
        public void compute(Vertex<K, Tuple2<K,K[][]>> vertex, MessageIterator<Tuple2<K,K[][]>> ks) throws Exception {


            if(getSuperstepNumber() == 1){

                System.out.println("" + vertex.getId() + "  " + vertex.getValue().toString());

                sendMessageToAllNeighbors((vertex.getValue()));

            }

        }

    }

    public static void main(String[] args)throws Exception{


        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        Graph<Long,Tuple2<Long,Long[][]>,NullValue> graph = Graph.fromDataSet(ListTestData.getDefaultVerticsSet(env),ListTestData.getDefaultEdges(env),env);

        DataSet<Vertex<Long,Tuple2<Long,Long[][]>>> result = new ListTest<Long,NullValue>().run(graph);

        result.print();
    }


}
