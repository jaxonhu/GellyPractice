package example.SG;


import example.data.ModularityTestData;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.undirected.VertexDegree;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/9/21
 * @Time: 上午11:01
 * @Project: GellyPractice
 */



/**
 * @param K: the vertex id
 * @param Long: the label of vertex
 * @param EV: the edge value
 */
public class Modularity<K,EV> {

    public Graph<K,Long,EV> graph;

    // total edges
    public long edgesnum = 0;


    //intern edges of each block
    public HashMap<Long,Long> interns;

    //degrees of each block;
    public HashMap<Long,Long> degrees;


    public Modularity(Graph<K,Long,EV> graph) {

        this.interns = new HashMap<>();
        this.degrees = new HashMap<>();
        this.graph = graph;
    }

    public  double caculate()throws Exception{

        DataSet<Vertex<K,Long>> vertices = graph.getVertices();

        DataSet<Edge<K,EV>> edges = graph.getEdges();

        List<Edge<K,EV>> edgelist = edges.collect();
        for(Iterator<Edge<K,EV>> iter = edgelist.iterator();iter.hasNext();){
            System.out.println(iter.next().toString());
        }

        //total edges num of a graph
        edgesnum = this.graph.numberOfEdges();

        //count each vertex's degrees
        DataSet<Vertex<K,LongValue>> degrees = new VertexDegree<K,Long,EV>().run(this.graph);

        DataSet<Block<Long>> blocks =  degrees
                .join(vertices)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Vertex<K, LongValue>, Vertex<K, Long>, Tuple3<K,Long,Long>>() {
                    @Override
                    public Tuple3<K,Long,Long> join(Vertex<K, LongValue> v1, Vertex<K, Long> v2) throws Exception {
                        return new Tuple3<K,Long,Long>(v1.f0,v2.f1,v1.f1.getValue());
                    }
                }).groupBy(1).reduce(new ReduceFunction<Tuple3<K,Long,Long>>() {
                    @Override
                    public Tuple3<K,Long,Long> reduce(Tuple3<K,Long,Long> t1, Tuple3<K,Long,Long> t2) throws Exception {
                        return new Tuple3<K,Long,Long>(t1.f0,t1.f1,t1.f2 + t2.f2);
                    }
                })
                .map(new MapFunction<Tuple3<K,Long,Long>, Block<Long>>() {
                    @Override
                    public Block map(Tuple3<K,Long,Long> t) throws Exception {
                        return new Block<>(t.f1,t.f2,0);
                    }
                });



        DataSet<Block<Long>> finalblocks = edges.join(vertices)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Edge<K,EV>, Vertex<K,Long>, Tuple5<K,K,Long,Long,Long>>() {
                    @Override
                    public Tuple5<K, K, Long, Long, Long> join(Edge<K, EV> e, Vertex<K, Long> v) throws Exception {

                        return new Tuple5<>(e.f0,e.f1,v.getValue(), 0L,1L);
                    }
                })
                .join(vertices)
                .where(1)
                .equalTo(0)
                .with(new JoinFunction<Tuple5<K,K,Long,Long,Long>, Vertex<K,Long>, Tuple5<K,K,Long,Long,Long>>() {
                    @Override
                    public Tuple5<K, K, Long, Long, Long> join(Tuple5<K, K, Long, Long, Long> t, Vertex<K, Long> v) throws Exception {
                        return new Tuple5<>(t.f0,t.f1,t.f2,v.getValue(),1L);
                    }
                })
                .filter(new FilterFunction<Tuple5<K, K, Long, Long, Long>>() {
                    @Override
                    public boolean filter(Tuple5<K, K, Long, Long, Long> t) throws Exception {
                        if(t.f2 == t.f3)
                            return true;
                        return false;

                    }
                })
                .groupBy(2)
                .reduce(new ReduceFunction<Tuple5<K, K, Long, Long, Long>>() {
                    @Override
                    public Tuple5<K, K, Long, Long, Long> reduce(Tuple5<K, K, Long, Long, Long> t1, Tuple5<K, K, Long, Long, Long> t2) throws Exception {
                        return new Tuple5<>(t1.f0,t1.f1,t1.f2,t1.f3,t1.f4 + t2.f4);
                    }
                })
                .join(blocks)
                .where(2)
                .equalTo("blockId")
                .with(new JoinFunction<Tuple5<K,K,Long,Long,Long>, Block<Long>, Block<Long>>() {
                    @Override
                    public Block<Long> join(Tuple5<K, K, Long, Long, Long> t, Block<Long> block) throws Exception {
                        return new Block<Long>((Long)block.blockId,block.totalDegrees,t.f4);
                    }
                });

        DataSet<Tuple2<K,Long>> tupples = vertices.map(new MapFunction<Vertex<K, Long>, Tuple2<K, Long>>() {
            @Override
            public Tuple2<K, Long> map(Vertex<K, Long> vertex) throws Exception {
                return new Tuple2<>(vertex.getId(),vertex.getValue());
            }
        });


        List<Block<Long>> fblist = finalblocks.collect();

        long s2 = 0;
        for(int i = 0; i < fblist.size() ; i++){
            s2 += fblist.get(i).InsideEdges;
        }

        double modularity = (double) s2/edgesnum;


        for(int j = 0; j < fblist.size(); j++){
            double tmp = (double)fblist.get(j).totalDegrees/edgesnum;
            modularity -=  (double) Math.pow(tmp,2);
        }


        return modularity;
    }


    // a partitioned social block
    // blockId differ from each other
    // totalDegrees : sum of vertex's  degree in a block
    // InsideEdges : sum of u  edges in a block
    public static  class Block<K>{

        private K blockId;

        private long totalDegrees;

        private long InsideEdges;


        public Block() {

        }

        public Block(K blockId, long totalDegrees, long InsideEdges) {
            this.blockId = blockId;
            this.totalDegrees = totalDegrees;
            this.InsideEdges = InsideEdges;
        }


        public K getBlockId() {
            return blockId;
        }



        public void setBlockId(K blockId) {
            this.blockId = blockId;
        }



        public long getTotalDegrees() {
            return totalDegrees;
        }

        public void setTotalDegrees(long totalDegrees) {
            this.totalDegrees = totalDegrees;
        }

        public long getInsideEdges() {
            return InsideEdges;
        }

        public void setInsideEdges(long InsideEdges) {
            this.InsideEdges = InsideEdges;
        }


        @Override
        public String toString() {
            return "Block{" +
                    "blockId=" + blockId +
                    ", totalDegrees=" + totalDegrees +
                    ", InsideEdges=" + InsideEdges +
                    '}';
        }
    }


    public   class VertexEdgeJoin1 implements JoinFunction<Edge,Vertex,Tuple5<K,K,Long,Long,Long>> {
        @Override
        public Tuple5<K, K, Long, Long, Long> join(Edge edge, Vertex vertex) throws Exception {

            return new Tuple5<>((K)edge.f0,(K)edge.f1,(Long)vertex.getValue(), 0L,1L);

        }
    }

    public static void main(String[] args)throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        Graph<Long,Long,Long> graph = new ModularityTestData().getDefaultGraph(env).getUndirected();


        double modularity = new Modularity<Long,Long>(graph).caculate();

        System.out.println(modularity);

    }

}
