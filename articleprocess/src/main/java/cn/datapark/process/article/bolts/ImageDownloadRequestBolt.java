package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.datapark.process.article.avro.encoder.ImageDownloadRequestAvroEncoder;
import cn.datapark.process.article.avro.encoder.ImageDownloadRequestAvroEncoderUtil;
import cn.datapark.process.article.model.ImageDownloadRequest;
import cn.datapark.process.article.util.ConstantUtil;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * Created by eason on 16/1/12.
 * 像kafka中发送需要下载的正文中的图片
 */
public class ImageDownloadRequestBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ImageDownloadRequestBolt.class);

    private OutputCollector collector;

    private static final String ImageAvrofile = "image_schema.avsc";
    private static final String ImageAlbumAvrofile = "album_schema_kafka.avsc";

    //生成avro的schema
    public static String WriterSchemaName = "cn.datapark.avro.kafkaalbummeta";
    //存储的schema
    public static String ReaderSchemaName = "cn.datapark.avro.kafkaalbummeta";


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        collector = outputCollector;

        ArrayList<InputStream> alScehma= new ArrayList<InputStream>();

        //按子类到父类的顺序添加schema
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ImageAvrofile));
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ImageAlbumAvrofile));

        ImageDownloadRequestAvroEncoder idrae = ImageDownloadRequestAvroEncoderUtil.getInstance();
        idrae.parseSchema(alScehma,WriterSchemaName,ReaderSchemaName);
    }

    public void execute(Tuple tuple) {


        ImageDownloadRequest idr;
        Object obj =  tuple.getValueByField(ConstantUtil.StreamFields.ARTICLEIMAGEDOWNLOADREQUEST);
        if (obj instanceof ImageDownloadRequest) {
            idr = (ImageDownloadRequest) obj;
        } else {
//            LOG.error("failed to get ImageDownloadRequest from tuple");
            collector.ack(tuple);
            return;
        }
        //正文内容中如果没有图片,则不发送请求
        if(idr.urlMap.size()==0){
//            LOG.info("no image send with article,src_url:"+idr.srcUrl);
            collector.ack(tuple);
            return;
        }

        ImageDownloadRequestAvroEncoder idrae = ImageDownloadRequestAvroEncoderUtil.getInstance();
        Map<String,String> map = idrae.build(idr);

        Set<String> keys = map.keySet();
        for(String key:keys){
//            LOG.info("send image download request,src_url:"+idr.srcUrl);
            collector.emit(ConstantUtil.Stream.IMAGE_DOWNLOAD_REQUEST_TO_KAFKA,tuple,new Values(key,map.get(key)));
        }
        collector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(ConstantUtil.Stream.IMAGE_DOWNLOAD_REQUEST_TO_KAFKA,
                new Fields(ConstantUtil.StreamFields.KAFKABOLTKEY,ConstantUtil.StreamFields.KAFKABOLTMESSAGE));
    }

}
