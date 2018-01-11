package example.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/8/31
 * @Time: 下午3:26
 * @Project: GellyPractice
 */
public class ListTestData {


    private static int initialSize  = 10;

    public static final DataSet<Edge<Long,NullValue>> getDefaultEdges(ExecutionEnvironment env){

        List<Edge<Long,NullValue>> list = new ArrayList<>();

        list.add(new Edge<>(1L,2L,NullValue.getInstance()));
        list.add(new Edge<>(1L,5L,NullValue.getInstance()));
        list.add(new Edge<>(1L,3L,NullValue.getInstance()));
        list.add(new Edge<>(2L,4L,NullValue.getInstance()));
        list.add(new Edge<>(2L,3L,NullValue.getInstance()));
        list.add(new Edge<>(2L,5L,NullValue.getInstance()));
        list.add(new Edge<>(3L,4L,NullValue.getInstance()));
        list.add(new Edge<>(3L,5L,NullValue.getInstance()));

        return env.fromCollection(list);

    }


    public static final DataSet<Vertex<Long,Tuple2<Long,Long[][]>>> getDefaultVerticsSet(ExecutionEnvironment env){

        List<Vertex<Long,Tuple2<Long,Long[][]>>> vertices = new ArrayList<>();

        vertices.add(new Vertex<>(1L,new Tuple2<Long,Long[][]>(1L,new Long[10][10])));
        vertices.add(new Vertex<>(2L,new Tuple2<Long,Long[][]>(2L,new Long[10][10])));
        vertices.add(new Vertex<>(3L,new Tuple2<Long,Long[][]>(3L,new Long[10][10])));
        vertices.add(new Vertex<>(4L,new Tuple2<Long,Long[][]>(4L,new Long[10][10])));
        vertices.add(new Vertex<>(5L,new Tuple2<Long,Long[][]>(5L,new Long[10][10])));
        vertices.add(new Vertex<>(6L,new Tuple2<Long,Long[][]>(6L,new Long[10][10])));

        return env.fromCollection(vertices);

    }


    public static final DataSet<Vertex<Long,Long[][]>> getDefaultVerticesSet2(ExecutionEnvironment env){

        List<Vertex<Long,Long[][]>> vertices = new ArrayList<>();

        vertices.add(new Vertex<Long, Long[][]>(1L,new Long[10][10]));
        vertices.add(new Vertex<Long, Long[][]>(2L,new Long[10][10]));
        vertices.add(new Vertex<Long, Long[][]>(3L,new Long[10][10]));
        vertices.add(new Vertex<Long, Long[][]>(4L,new Long[10][10]));
        vertices.add(new Vertex<Long, Long[][]>(5L,new Long[10][10]));
        vertices.add(new Vertex<Long, Long[][]>(6L,new Long[10][10]));

        return env.fromCollection(vertices);
    }


    public static final DataSet<Vertex<Long,ArrayList<Long>>> getDefaultVerticesSet3(ExecutionEnvironment env){

        ArrayList<Vertex<Long,ArrayList<Long>>> vertices = new ArrayList<>();

        vertices.add(new Vertex<Long, ArrayList<Long>>(1L,new ArrayList<Long>(3)));
        vertices.add(new Vertex<Long, ArrayList<Long>>(2L,new ArrayList<Long>(3)));
        vertices.add(new Vertex<Long, ArrayList<Long>>(3L,new ArrayList<Long>(3)));
        vertices.add(new Vertex<Long, ArrayList<Long>>(4L,new ArrayList<Long>(3)));
        vertices.add(new Vertex<Long, ArrayList<Long>>(5L,new ArrayList<Long>(3)));
        vertices.add(new Vertex<Long, ArrayList<Long>>(6L,new ArrayList<Long>(3)));

        return env.fromCollection(vertices);

    }


    public static final DataSet<Vertex<Long,ArrayList<List<Long>>>> getDefaultVerticesSet4(ExecutionEnvironment env){

        ArrayList<Vertex<Long,ArrayList<List<Long>>>> vertices = new ArrayList<>();

        vertices.add(new Vertex<Long, ArrayList<List<Long>>>(1L,new ArrayList<List<Long>>(3)));
        vertices.add(new Vertex<Long, ArrayList<List<Long>>>(2L,new ArrayList<List<Long>>(3)));
        vertices.add(new Vertex<Long, ArrayList<List<Long>>>(3L,new ArrayList<List<Long>>(3)));
        vertices.add(new Vertex<Long, ArrayList<List<Long>>>(4L,new ArrayList<List<Long>>(3)));
        vertices.add(new Vertex<Long, ArrayList<List<Long>>>(5L,new ArrayList<List<Long>>(3)));
        vertices.add(new Vertex<Long, ArrayList<List<Long>>>(6L,new ArrayList<List<Long>>(3)));

        return env.fromCollection(vertices);

    }


}
