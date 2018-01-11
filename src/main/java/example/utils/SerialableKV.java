package example.utils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.MapValue;
import org.apache.flink.types.Value;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/9/28
 * @Time: 下午9:15
 * @Project: GellyPractice
 */
public class SerialableKV extends MapValue<LongValue,LongValue> {

    private Map<LongValue,LongValue> map;

    public SerialableKV(Map<LongValue, LongValue> map) {
        super(map);
        this.map = map;
    }

    @Override
    public void write(DataOutputView out) throws IOException {

        Set<Entry<LongValue,LongValue>> entries =  map.entrySet();
        out.writeInt(map.size());
        for(Entry<LongValue,LongValue> entry : entries){
            out.writeLong(entry.getKey().getValue());
            out.writeLong(entry.getValue().getValue());
        }

    }

    @Override
    public void read(DataInputView in) throws IOException {
        int length = in.readInt();
        Map<LongValue,LongValue> map = new HashMap<LongValue,LongValue>();

        for(int i = 0 ; i < length ; i++){
            map.put(new LongValue(in.readLong()),new LongValue(in.readLong()));
        }

        this.map = map;
    }
}
