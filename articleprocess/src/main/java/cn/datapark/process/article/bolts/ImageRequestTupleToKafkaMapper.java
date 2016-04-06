package cn.datapark.process.article.bolts;

import backtype.storm.tuple.Tuple;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;

/**
 * Created by eason on 16/1/18.
 */
public class ImageRequestTupleToKafkaMapper implements TupleToKafkaMapper {
    public Object getKeyFromTuple(Tuple tuple) {
        return null;
    }

    public Object getMessageFromTuple(Tuple tuple) {
        return null;
    }
}
