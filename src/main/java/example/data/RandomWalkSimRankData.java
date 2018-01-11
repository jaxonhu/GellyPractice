package example.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;


import java.util.ArrayList;
import java.util.List;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/10/27
 * @Time: 上午9:34
 * @Project: GellyPractice
 */
public class RandomWalkSimRankData {


    public static   final DataSet<Vertex<Long,List<Long>>> getDefaultVertexDataset(ExecutionEnvironment env){

        List<Vertex<Long,List<Long>>>  vertices = new ArrayList<>();

        vertices.add(new Vertex<Long,List<Long>>(1L,new ArrayList<Long>()));
        vertices.add(new Vertex<Long,List<Long>>(2L,new ArrayList<Long>()));
        vertices.add(new Vertex<Long,List<Long>>(3L,new ArrayList<Long>()));
        vertices.add(new Vertex<Long,List<Long>>(4L,new ArrayList<Long>()));
        vertices.add(new Vertex<Long,List<Long>>(5L,new ArrayList<Long>()));
        vertices.add(new Vertex<Long,List<Long>>(6L,new ArrayList<Long>()));


        return env.fromCollection(vertices);
    }


    public static  final DataSet<Edge<Long,NullValue>> getDefaultEdgeDataset(ExecutionEnvironment env){
        List<Edge<Long,NullValue>> edges = new ArrayList<>();

        edges.add(new Edge<Long,NullValue>(1L,2L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(1L,4L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(1L,3L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(1L,5L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(2L,1L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(2L,4L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(3L,1L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(3L,4L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(4L,2L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(4L,3L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(5L,2L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(5L,4L,new NullValue()));
        edges.add(new Edge<Long,NullValue>(6L,1L,new NullValue()));

        return env.fromCollection(edges);

    }



}
