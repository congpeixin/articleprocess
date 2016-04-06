package cn.datapark.process.article.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import cn.datapark.process.article.bolts.*;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.es.ESClient;
import cn.datapark.process.article.es.ESUtil;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClient;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClientUtil;
import cn.datapark.process.article.hbase.RawArticleHBaseClient;
import cn.datapark.process.article.hbase.RawArticleHBaseClientUtil;
import cn.datapark.process.article.model.ArticleSet;
import cn.datapark.process.article.model.ImageDownloadRequest;
import cn.datapark.process.article.util.ConstantUtil;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Properties;

/**
 * Created by eason on 15/10/31.
 */
public class ArticleProcessTopology {
    private static final Logger LOG = Logger.getLogger(ArticleProcessTopology.class);


    public static void main(String[] args) throws Exception {

        DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();

        ArticleExtractTopoConfig articleExtractTopoConfig = null;

        ConfigUtil.initConfig(ArticleProcessTopology.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
        articleExtractTopoConfig = ConfigUtil.getConfigInstance();


        //创建索引
        Configuration conf = new BaseConfiguration();
        conf.addProperty(ESClient.CLUSTER_NAME, articleExtractTopoConfig.getESClusterName());
        conf.addProperty(ESClient.CLUSTER_IP, articleExtractTopoConfig.getESClusterIP());
        conf.addProperty(ESClient.CLUSTER_PORT, articleExtractTopoConfig.getESClusterPort());
        ESUtil.initESClient(conf);
        if( ESUtil.getESClientInstance().createMapping(articleExtractTopoConfig.getEsIndexName(),articleExtractTopoConfig.getESIndexMappingFileName())){
            LOG.info("——————————————————————————————————————————Create ES mapping("+articleExtractTopoConfig.getEsIndexName() +")suceed——————————————————————————————————————————————");
        }else{
            LOG.info("——————————————————————————————————————————Failed to create ES mapping("+articleExtractTopoConfig.getEsIndexName() +")————————————————————————————————————————————");
        }
        //创建正文提取hbase表
        try {
           ExtractedArticleHBaseClient extractedArticleHBaseClient = ExtractedArticleHBaseClientUtil.getClientInstance();
            extractedArticleHBaseClient.initClient(articleExtractTopoConfig);
            extractedArticleHBaseClient.createTable(
                    articleExtractTopoConfig.getExtractedHBaseNamespace(),
                    articleExtractTopoConfig.getExtractedHBaseArticleTableName(),
                    articleExtractTopoConfig.getExtractedHBaseTableColumnFamily());


        }catch (Exception e){
            e.printStackTrace();
        }

        //创建原始文章html内容hbase表
        try {
            RawArticleHBaseClient rawArticleHBaseClient = RawArticleHBaseClientUtil.getClientInstance();
            rawArticleHBaseClient.initClient(articleExtractTopoConfig);
            rawArticleHBaseClient.createTable(
                    articleExtractTopoConfig.getRawHBaseNamespace(),
                    articleExtractTopoConfig.getRawHBaseArticleTableName(),
                    articleExtractTopoConfig.getRawHBaseTableColumnFamily());
        }catch (Exception e){
            e.printStackTrace();
        }

        TopologyBuilder builder = new TopologyBuilder();

        //正文提取topo
        BrokerHosts articleExtractHosts =  new ZkHosts(articleExtractTopoConfig.getKafkaZookeeperServer());
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ArticleExtact BrokerHost:"+ articleExtractTopoConfig.getKafkaZookeeperServer());
        SpoutConfig articleExtactSpoutConfig = new SpoutConfig(articleExtractHosts,articleExtractTopoConfig.getArticleHtmlSourceTopic()
                ,articleExtractTopoConfig.getKafkaZookeeperRoot(),"articleextractprocess");
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>articleextract Kafkaspout config: root:"+articleExtactSpoutConfig.zkRoot);
        articleExtactSpoutConfig.scheme =  new SchemeAsMultiScheme(new StringScheme());
        articleExtactSpoutConfig.maxOffsetBehind = articleExtractTopoConfig.getConsumerMaxOffsetBehind();
        articleExtactSpoutConfig.useStartOffsetTimeIfOffsetOutOfRange = articleExtractTopoConfig.isConsumerUseStartOffsetTimeIfOffSetOutOfRange();
        articleExtactSpoutConfig.forceFromStart = articleExtractTopoConfig.isConsumerForceFromStart();
        articleExtactSpoutConfig.startOffsetTime = articleExtractTopoConfig.getConsumerStartOffsetTime();

        KafkaSpout articleExtactKafkaSpout = new KafkaSpout(articleExtactSpoutConfig);
        builder.setSpout(ConstantUtil.Bolt.ARTICLESOURCE_KAFKASPOUT, articleExtactKafkaSpout, 1);
        builder.setBolt(ConstantUtil.Bolt.ARTICLECONTENT_EXTRACTBOLT, new ArticleContentExtractBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.ARTICLESOURCE_KAFKASPOUT);

        builder.setBolt(ConstantUtil.Bolt.IMAGEDOWNLOAD_REQUESTBOLT, new ImageDownloadRequestBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.ARTICLECONTENT_EXTRACTBOLT,ConstantUtil.Stream.IMAGE_DOWNLOAD_REQUEST);

        builder.setBolt(ConstantUtil.Bolt.EXTRACTEDCONTECT_STOREBOLT,new ExtractedContentStoreBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.ARTICLECONTENT_EXTRACTBOLT,ConstantUtil.Stream.EXTRACTED_ARTICLE);

        builder.setBolt(ConstantUtil.Bolt.EXTRACTEDCONTECT_INDEXBOLT,new ExtractedContentIndexBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.ARTICLECONTENT_EXTRACTBOLT,ConstantUtil.Stream.EXTRACTED_ARTICLE);

        KafkaBolt kafkaBolt = new KafkaBolt<String,String>();
                kafkaBolt.withTopicSelector(new DefaultTopicSelector(articleExtractTopoConfig.getImageDownloadRequestTopic()))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
        builder.setBolt(ConstantUtil.Bolt.KAFKABOLT,kafkaBolt,1)
                .shuffleGrouping(ConstantUtil.Bolt.IMAGEDOWNLOAD_REQUESTBOLT
                        ,ConstantUtil.Stream.IMAGE_DOWNLOAD_REQUEST_TO_KAFKA);


        //图片url更新拓扑
        BrokerHosts imageUpdateHosts =  new ZkHosts(articleExtractTopoConfig.getKafkaZookeeperServer());
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BrokerHost:"+ articleExtractTopoConfig.getKafkaZookeeperServer());
        SpoutConfig spoutConfig = new SpoutConfig(imageUpdateHosts,articleExtractTopoConfig.getImageAlbumMetaTopic()
                ,articleExtractTopoConfig.getKafkaZookeeperRoot(),"articleimageurlupdateprocess");
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Kafkaspout config: root:"+spoutConfig.zkRoot);
        spoutConfig.scheme =  new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.maxOffsetBehind = articleExtractTopoConfig.getImageAlbumMetaSpoutMaxOffsetBehind();
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = articleExtractTopoConfig.isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange();
        spoutConfig.forceFromStart = articleExtractTopoConfig.isImageAlbumMetaSpoutForceFromStart();
        spoutConfig.startOffsetTime = articleExtractTopoConfig.getImageAlbumMetaSpoutStartOffsetTime();

        //buildFromAvroBinary topology
        KafkaSpout imageUpdateKafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout(ConstantUtil.Bolt.IMAGEUPDATE_KAFKASPOUT, imageUpdateKafkaSpout, 1);
        builder.setBolt(ConstantUtil.Bolt.CRAWLEDIMAGEALBUMAVROBOLT, new CrawledImageAlbumAvroBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.IMAGEUPDATE_KAFKASPOUT);

        builder.setBolt(ConstantUtil.Bolt.IMAGEDOWNLOADEDARTICLEUPDATEBOLT, new ImageDownloadedArticleUpdateBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.CRAWLEDIMAGEALBUMAVROBOLT,
                        ConstantUtil.Stream.CRAWLED_IMAGE_ALBUM);


        //原始文章Web页面存储bolt
        builder.setBolt(ConstantUtil.Bolt.RAWAARTICLE_STOREBOLT, new RawWebStoreBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.ARTICLESOURCE_KAFKASPOUT);



        //set producer properties.
        Properties props = new Properties();
        props.put("metadata.broker.list", articleExtractTopoConfig.getImageRequestKafkaBrokerList());
        props.put("request.required.acks", articleExtractTopoConfig.getImageRequestKafkaRequiredAcks());
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        Config cfg = new Config();
        cfg.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
        cfg.put("topic",articleExtractTopoConfig.getImageDownloadRequestTopic());

        //注册自定义serializer
        cfg.registerSerialization(ArticleSet.class, ArticleSetSerializer.class);
        cfg.registerSerialization(ImageDownloadRequest.class, ImageDownloadRequestSerializer.class);

        if(args != null && args.length > 0){
            cfg.setDebug(false);
            cfg.setNumWorkers(16);
            cfg.setNumAckers(4);
            cfg.setMaxSpoutPending(2);
            cfg.setMessageTimeoutSecs(30);
            StormSubmitter.submitTopology(args[0], cfg, builder.createTopology());

        }else {
            //local debug mode
            cfg.setDebug(false);
            //LocalCluster cluster = new LocalCluster(config.getString("zookeeper.ip"),Long.valueOf(config.getString("zookeeper.port")));
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("articleprocesstopo", cfg, builder.createTopology());
            Utils.sleep(10000000);
            cluster.killTopology("articleprocesstopo");

            cluster.shutdown();
        }

        dcu.destroy();
    }
}
