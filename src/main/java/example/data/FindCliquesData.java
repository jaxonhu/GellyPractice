package example.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.List;
import java.util.List;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/8/26
 * @Time: AM 11:01
 */
public class FindCliquesData<K,V> {




    public static final DataSet<Edge<Long, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env){

        List<Edge<Long, NullValue>> list = new ArrayList<>();

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


    public static final DataSet<Vertex<Long,ArrayList<List<Long>>>> getDefaultVerticsSet(ExecutionEnvironment env){

        ArrayList<Vertex<Long,ArrayList<List<Long>>>> verteics = new ArrayList<>();

        verteics.add(new Vertex<>(1L,new ArrayList<List<Long>>(10)));
        verteics.add(new Vertex<>(2L,new ArrayList<List<Long>>(10)));
        verteics.add(new Vertex<>(3L,new ArrayList<List<Long>>(10)));
        verteics.add(new Vertex<>(4L,new ArrayList<List<Long>>(10)));
        verteics.add(new Vertex<>(5L,new ArrayList<List<Long>>(10)));

        return env.fromCollection(verteics);

     }

    private FindCliquesData(){};


}



