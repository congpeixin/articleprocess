package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.datapark.process.article.avro.encoder.ImageDownloadRequestAvroEncoder;
import cn.datapark.process.article.es.ESClient;
import cn.datapark.process.article.util.ConstantUtil;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import cn.datapark.process.core.image.ImageAlbumMeta;
import cn.datapark.process.core.image.ImageAlbumMetaBuilder;
import cn.datapark.process.core.utils.ESUtil;
import kafka.message.Message;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by eason on 15/10/31.
 * 按照ES Mapping格式化数据，并判断图集是否存在于es中
 */
public class CrawledImageAlbumAvroBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(CrawledImageAlbumAvroBolt.class);

    DistributedConfigUtil dcu = null;

    private OutputCollector collector;

    public static final String KafkaAvroSchemaFile = "album_schema_kafka.avsc";
    public static final String ESAvroSchemaFile = "album_schema_es.avsc";
    public static final String ImageSchemaFile = "image_schema.avsc";

    private boolean isAvroBinary = false;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        this.collector = outputCollector;

        dcu = DistributedConfigUtil.getInstance();

        ArrayList<InputStream>  alScehma= new ArrayList<InputStream>();

        //按子类到父类的顺序添加schema
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ImageSchemaFile));
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ESAvroSchemaFile));
        alScehma.add(getClass().getClassLoader().getResourceAsStream(KafkaAvroSchemaFile));

        ImageAlbumMetaBuilder.parseSchema(alScehma);

    }

    public void execute(Tuple tuple) {

        try{

            ImageAlbumMeta imageAlbumMeta;


            if(isAvroBinary){
                //处理二进制avro
                Message message = new Message(tuple.getBinary(0));

                ByteBuffer bb = message.payload();

                byte[] buffer = new byte[bb.remaining()];
                bb.get(buffer, 0, buffer.length);

                imageAlbumMeta = ImageAlbumMetaBuilder.buildFromAvroBinary(buffer);
            }else{
                //处理文本avro
                imageAlbumMeta = ImageAlbumMetaBuilder.buildFromJsonString(tuple.getString(0));

            }

            if(!imageAlbumMeta.getType().equalsIgnoreCase(ImageDownloadRequestAvroEncoder.TYPE_VALUE)){
//                LOG.info("get update message type:"+imageAlbumMeta.getType()+",ignore");
                collector.ack(tuple);
                return;
            }

//            LOG.info("get articleset update message,sending to next bolt,src_url:"+imageAlbumMeta.getSrcURL());
            collector.emit(ConstantUtil.Stream.CRAWLED_IMAGE_ALBUM, tuple,
                    new Values(ESClient.buildESDocIDFromURL(imageAlbumMeta.getSrcId()),
                    imageAlbumMeta.toFiltedString(),
                    imageAlbumMeta.getType()));



        }catch (Exception e){
            e.printStackTrace();
        }
        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(ConstantUtil.Stream.CRAWLED_IMAGE_ALBUM,
                new Fields(ConstantUtil.StreamFields.DocID,
                        ConstantUtil.StreamFields.ImageAlbumMeta,
                        ConstantUtil.StreamFields.ImageAlbumType));
/*        outputFieldsDeclarer.declare(new Fields(ConstantUtil.StreamFields.DocID,
                ConstantUtil.StreamFields.ImageAlbumMeta,
                ConstantUtil.StreamFields.ImageAlbumType));*/



    }

    @Override
    public void cleanup() {
        ESUtil.getInstance().close();
        super.cleanup();
    }
}
