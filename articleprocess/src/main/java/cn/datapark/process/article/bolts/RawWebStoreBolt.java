package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.datapark.process.article.avro.decoder.ArticleSetAvroDecoder;
import cn.datapark.process.article.avro.decoder.ArticleSetAvroDecoderUtil;
import cn.datapark.process.article.avro.decoder.ArticleSetAvroRecord;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.hbase.RawArticleHBaseClient;
import cn.datapark.process.article.hbase.RawArticleHBaseClientUtil;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import kafka.message.Message;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by eason on 16/1/12.
 * 原始Web页面存储到Hbase中
 */
public class RawWebStoreBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(RawWebStoreBolt.class);

    private OutputCollector collector;

    private RawArticleHBaseClient rawHBaseClient;

    boolean isAvroBinary = false;

    private static final String ArticleKafkaAvrofile = "article_schema_kafka.avsc";
    private static final String ArticleSetKafkaAvrofile = "articleset_schema_kafka.avsc";

    //生成avro的schema
    public static String WriterSchemaName = "cn.datapark.avro.kafkaarticleset";
    //存储的schema
    public static String ReaderSchemaName = "cn.datapark.avro.kafkaarticleset";

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        collector = outputCollector;
        DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();
        try {
            ConfigUtil.initConfig(RawWebStoreBolt.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
            ArticleExtractTopoConfig topoConfig = ConfigUtil.getConfigInstance();

            collector = outputCollector;
            rawHBaseClient = RawArticleHBaseClientUtil.getClientInstance();
            rawHBaseClient.initClient(topoConfig);

        }catch (Exception e){
            e.printStackTrace();
        }

        //初始化schema
        ArrayList<InputStream> alScehma = new ArrayList<InputStream>();

        //按子类到父类的顺序添加schema
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ArticleKafkaAvrofile));
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ArticleSetKafkaAvrofile));

        ArticleSetAvroDecoder asab = ArticleSetAvroDecoderUtil.getInstance();

        asab.parseSchema(alScehma, WriterSchemaName, ReaderSchemaName);

    }

    public void execute(Tuple tuple) {

        ArticleSetAvroRecord asar;
        try {
            if (isAvroBinary) {
                //处理二进制avro
                Message message = new Message(tuple.getBinary(0));

                ByteBuffer bb = message.payload();

                byte[] buffer = new byte[bb.remaining()];
                bb.get(buffer, 0, buffer.length);

                asar = ArticleSetAvroDecoderUtil.getInstance().buildArticleSetFromAvroBinary(buffer);

            } else {
                //处理文本avro
                asar = ArticleSetAvroDecoderUtil.getInstance().buildArticleSetFromJsonString(tuple.getString(0));
            }

            rawHBaseClient.putRawArticle(asar);
//            LOG.info("raw web saved ,src_url:"+asar.getArticleSrcURL());
        } catch (Exception e) {
            e.printStackTrace();
        }

        collector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
