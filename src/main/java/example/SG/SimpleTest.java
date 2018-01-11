package example.SG;

import example.data.ListTestData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.pregel.ComputeFunction;
import org.apache.flink.graph.pregel.MessageIterator;
import org.apache.flink.types.NullValue;

import java.util.*;

import java.util.Arrays;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/8/31
 * @Time: 下午6:38
 * @Project: GellyPractice
 */
public class SimpleTest<K,EV> implements GraphAlgorithm<K,ArrayList<List<Long>>,EV,DataSet<Vertex<K,ArrayList<List<Long>>>>> {


    @Override
    public DataSet<Vertex<K, ArrayList<List<Long>>>> run(Graph<K, ArrayList<List<Long>>, EV> input) throws Exception {





        return input.runVertexCentricIteration(new TestComputeFunction<K, EV>(),null,10).getVertices();
    }


    public static class TestComputeFunction<K,EV> extends ComputeFunction<K,ArrayList<List<Long>>,EV,MessagePassBy>{

        @Override
        public void compute(Vertex<K, ArrayList<List<Long>>> vertex, MessageIterator<MessagePassBy> longs) throws Exception {

            if(getSuperstepNumber() == 1){

                ArrayList<List<Long>> list = new ArrayList<>();
                ArrayList<Long>  sublist = new ArrayList<>();
                sublist.add((Long)vertex.getId());
                //list[0][0] = (Long)vertex.getId();
                list.add(sublist);
                setNewVertexValue(list);
                MessagePassBy msg = new MessagePassBy();
                msg.msg.add(sublist);
                System.out.println("" + vertex.getValue());
                sendMessageToAllNeighbors(msg);
            }else{

                for(;longs.hasNext();){
                    MessagePassBy msgs = longs.next();
                    System.out.println("hello srcId= " + vertex.getId() + "  SuperStep=" + getSuperstepNumber()+"  " + Arrays.deepToString(msgs.msg.toArray()));
                }
            }

        }
    }


    public static  class MessagePassBy{

        public List<List<Long>> msg = new ArrayList<>();

        public MessagePassBy() {
        }


    }

    public static void main(String[] args)throws Exception{


        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        Graph<Long,ArrayList<List<Long>>,NullValue> graph = Graph.fromDataSet(ListTestData.getDefaultVerticesSet4(env),ListTestData.getDefaultEdges(env),env)
                .getUndirected();

        DataSet<Vertex<Long,ArrayList<List<Long>>>>  result = new SimpleTest<Long,NullValue>().run(graph);

        result.print();


    }
}
