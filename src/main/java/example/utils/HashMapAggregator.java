package example.utils;

import org.apache.flink.api.common.aggregators.Aggregator;

import java.util.HashMap;

/**
 * @Author: jaxon
 * @Description:
 * @Date: 2017/9/28
 * @Time: 下午9:12
 * @Project: GellyPractice
 */
public class HashMapAggregator  implements Aggregator<SerialableKV> {

    @Override
    public SerialableKV getAggregate() {
        return null;
    }

    @Override
    public void aggregate(SerialableKV element) {

    }

    @Override
    public void reset() {

    }
}
