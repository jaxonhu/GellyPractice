package example.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.runtime.io.network.api.reader.BufferReader;
import org.apache.flink.types.NullValue;
import scala.xml.Null;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/11/22
 * @Time: 上午10:25
 * @Project: GellyPractice
 */
public class ReadFromText<K,V> {

    public static final DataSet<Edge<Long,NullValue>> getEdgesFromText(String path, ExecutionEnvironment env){

        List<Edge<Long,NullValue>> list = new ArrayList<Edge<Long,NullValue>>();

        try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            String s;
            StringBuilder sb = new StringBuilder();
            while((s = in.readLine()) != null){
                if(s.contains("#")) continue;
                String fromId = s.split("\\s+")[0];
                String toId = s.split("\\s+")[1];
                long from = Long.valueOf(fromId);
                long to = Long.valueOf(toId);
                list.add(new Edge<Long,NullValue>(from,to,new NullValue()));
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

        return  env.fromCollection(list);
    }


 public static final DataSet<Edge<Long,NullValue>> getEdgesFromFile(String path, ExecutionEnvironment env){

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




    public static final DataSet<Vertex<Long,ArrayList<List<Long>>>> getVerteicsFromText(String path, ExecutionEnvironment env){

        List<Vertex<Long,ArrayList<List<Long>>>> list = new ArrayList<>();

        HashSet<Long> ids = new HashSet<>();

         try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            String s;
            StringBuilder sb = new StringBuilder();
            while((s = in.readLine()) != null){
                String fromId = s.split("\\s+")[0];
                String toId = s.split("\\s+")[1];
                long from = Long.valueOf(fromId);
                long to = Long.valueOf(toId);
                ids.add(from);
                ids.add(to);
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }

        for(Long value : ids){
            list.add(new Vertex<>(value,new ArrayList<List<Long>>()));
        }



        return env.fromCollection(list);
    }

}
