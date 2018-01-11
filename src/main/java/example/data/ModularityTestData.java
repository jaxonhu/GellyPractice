package example.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import sun.jvm.hotspot.memory.EdenSpace;


import java.util.ArrayList;
import java.util.List;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/9/24
 * @Time: 上午9:29
 * @Project: GellyPractice
 */
public class ModularityTestData {


    public DataSet<Vertex<Long,Long>>  getDefaultVertices(ExecutionEnvironment env){

        List<Vertex<Long,Long>> list = new ArrayList<>();

        list.add(new Vertex<Long,Long>(1L,1L));
        list.add(new Vertex<Long,Long>(2L,1L));
        list.add(new Vertex<Long,Long>(3L,1L));
        list.add(new Vertex<Long,Long>(0L,1L));

        list.add(new Vertex<Long,Long>(4L,2L));
        list.add(new Vertex<Long,Long>(5L,2L));
        list.add(new Vertex<Long,Long>(6L,2L));
        list.add(new Vertex<Long,Long>(7L,2L));
        list.add(new Vertex<Long,Long>(8L,2L));

        list.add(new Vertex<Long,Long>(9L,3L));
        list.add(new Vertex<Long,Long>(10L,3L));
        list.add(new Vertex<Long,Long>(11L,3L));
        list.add(new Vertex<Long,Long>(12L,3L));
        list.add(new Vertex<Long,Long>(13L,3L));

        return env.fromCollection(list);

    }

    public DataSet<Edge<Long,Long>> getDefaultEdges(ExecutionEnvironment env){

        List<Edge<Long,Long>> list = new ArrayList<>();

        list.add(new Edge<Long, Long>(1L,2L,1L));
        list.add(new Edge<Long, Long>(1L,0L,1L));
        list.add(new Edge<Long, Long>(1L,3L,1L));
        list.add(new Edge<Long, Long>(2L,0L,1L));
        list.add(new Edge<Long, Long>(2L,4L,1L));
        list.add(new Edge<Long, Long>(3L,0L,1L));
        list.add(new Edge<Long, Long>(4L,5L,1L));
        list.add(new Edge<Long, Long>(4L,7L,1L));
        list.add(new Edge<Long, Long>(5L,8L,1L));
        list.add(new Edge<Long, Long>(5L,6L,1L));
        list.add(new Edge<Long, Long>(5L,7L,1L));
        list.add(new Edge<Long, Long>(6L,8L,1L));
        list.add(new Edge<Long, Long>(7L,8L,1L));
        list.add(new Edge<Long, Long>(7L,10L,1L));
        list.add(new Edge<Long, Long>(8L,9L,1L));
        list.add(new Edge<Long, Long>(9L,10L,1L));
        list.add(new Edge<Long, Long>(9L,12L,1L));
        list.add(new Edge<Long, Long>(9L,13L,1L));
        list.add(new Edge<Long, Long>(10L,11L,1L));
        list.add(new Edge<Long, Long>(10L,12L,1L));
        list.add(new Edge<Long, Long>(11L,12L,1L));
        list.add(new Edge<Long, Long>(11L,13L,1L));
        list.add(new Edge<Long, Long>(12L,13L,1L));

        return env.fromCollection(list);
    }


    public Graph<Long,Long,Long> getDefaultGraph(ExecutionEnvironment env){

        return Graph.fromDataSet(this.getDefaultVertices(env),this.getDefaultEdges(env),env);

    }



}
