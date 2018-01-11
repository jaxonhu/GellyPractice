package example.data;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jaxon on 2017/8/15.
 */
public class GraphColoringData {


    public static final String EDGES = "1	2\n"+"1	3\n"+"2	3\n"+"2	6\n"+"3	4\n"+"3	5\n"+"3	6\n"+"4	5\n"+"6	7\n";

    public static DataSet<Edge<Long,NullValue>> getDefaultEdgeDatasets(ExecutionEnvironment env){

        List<Edge<Long,NullValue>> edges = new ArrayList<>();

        edges.add(new Edge<Long, NullValue>(1L, 2L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(1L, 3L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(2L, 3L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(2L, 6L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(3L, 4L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(3L, 5L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(3L, 6L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(4L, 5L, NullValue.getInstance()));
        edges.add(new Edge<Long, NullValue>(6L, 7L, NullValue.getInstance()));

        return env.fromCollection(edges);
    }

    public static DataSet<Vertex<Long,Long>> getDefaultVertexDatasets(ExecutionEnvironment env){

        //List<Vertex<Long,Long>> vertexs = new ArrayList<>();

        DataSet<Vertex<Long,Long>> vertexDataSet = getDefaultEdgeDatasets(env)
                .flatMap(new EdgeToVertexFunction<Long>()).distinct();

        return vertexDataSet;
    }

    public static DataSet<Edge<Long,NullValue>> getEdgeDataFromFile(String path, ExecutionEnvironment env){

        return env.readCsvFile(path)
                 .lineDelimiter("\n")
                 .fieldDelimiter("\t")
                 .ignoreComments("#")
                 .types(Long.class, Long.class)
                 .map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
                     @Override
                     public Edge<Long, NullValue> map(Tuple2<Long, Long> value) throws Exception {
                         return new Edge<>(value.f0,value.f1,new NullValue());
                     }
                 });
    }

    public static DataSet<Vertex<Long,Long>> getFromTextfile(ExecutionEnvironment env){
        return null;
    }

    public static final class EdgeToVertexFunction<K> implements FlatMapFunction<Edge<K,NullValue>,Vertex<K,Long>>{
        @Override
        public void flatMap(Edge<K, NullValue> edge, Collector<Vertex<K,Long>> collector) throws Exception {
            collector.collect(new Vertex<>(edge.f0,new Long(0)));
            collector.collect(new Vertex<>(edge.f1,new Long(0)));
        }
    }



}
