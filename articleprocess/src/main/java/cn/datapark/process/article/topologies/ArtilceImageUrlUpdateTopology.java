package cn.datapark.process.article.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import cn.datapark.process.article.bolts.CrawledImageAlbumAvroBolt;
import cn.datapark.process.article.bolts.ImageDownloadedArticleUpdateBolt;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.util.ConstantUtil;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import org.apache.log4j.Logger;
import storm.kafka.*;

/**
 * Created by eason on 15/10/31.
 */
public class ArtilceImageUrlUpdateTopology {
    private static final Logger LOG = Logger.getLogger(ArtilceImageUrlUpdateTopology.class);


    public static void main(String[] args) throws Exception {

        DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();

        ArticleExtractTopoConfig articleExtractTopoConfig = null;
        try {
            ConfigUtil.initConfig(ArtilceImageUrlUpdateTopology.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
            articleExtractTopoConfig = ConfigUtil.getConfigInstance();

        }catch (Exception e){
            e.printStackTrace();
        }
        //read kafka config
        //XMLConfiguration config = new XMLConfiguration(SkuProcessTopology.class.getClassLoader().getResource(KafkaUtil.KAFKA_CONFIG_FILEPATH));
        TopologyBuilder builder = new TopologyBuilder();

        //kafka config

        //XMLConfiguration config = new XMLConfiguration((KafkaUtil.KAFKA_CONFIG_FILEPATH));
        //BrokerHosts hosts =  new ZkHosts(config.getString("zookeeper.ip")+":"+config.getString("zookeeper.port"));
        //从baidu disconf 中读取kafka配置项
        //dcu.initContext();
        BrokerHosts hosts =  new ZkHosts(articleExtractTopoConfig.getKafkaZookeeperServer());
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>BrokerHost:"+ articleExtractTopoConfig.getKafkaZookeeperServer());
        SpoutConfig spoutConfig = new SpoutConfig(hosts,articleExtractTopoConfig.getImageAlbumMetaTopic()
                ,articleExtractTopoConfig.getKafkaZookeeperRoot(),"articleimageurlupdateprocess");
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Kafkaspout config: root:"+spoutConfig.zkRoot);
        spoutConfig.scheme =  new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.maxOffsetBehind = articleExtractTopoConfig.getImageAlbumMetaSpoutMaxOffsetBehind();
        spoutConfig.useStartOffsetTimeIfOffsetOutOfRange = articleExtractTopoConfig.isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange();
        spoutConfig.forceFromStart = articleExtractTopoConfig.isImageAlbumMetaSpoutForceFromStart();
        spoutConfig.startOffsetTime = articleExtractTopoConfig.getImageAlbumMetaSpoutStartOffsetTime();

        //buildFromAvroBinary topology
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout(ConstantUtil.Bolt.IMAGEUPDATE_KAFKASPOUT, kafkaSpout, 1);
        builder.setBolt(ConstantUtil.Bolt.CRAWLEDIMAGEALBUMAVROBOLT, new CrawledImageAlbumAvroBolt(),1)
                .shuffleGrouping(ConstantUtil.Bolt.IMAGEUPDATE_KAFKASPOUT);

        builder.setBolt(ConstantUtil.Bolt.IMAGEDOWNLOADEDARTICLEUPDATEBOLT, new ImageDownloadedArticleUpdateBolt(),1)
                .fieldsGrouping(ConstantUtil.Bolt.CRAWLEDIMAGEALBUMAVROBOLT,new Fields(ConstantUtil.StreamFields.ImageAlbumType));



        Config cfg = new Config();


        //注册自定义serializer
        //cfg.registerSerialization(ArticleSet.class, ArticleSetSerializer.class);

        if(args != null && args.length > 0){
            cfg.setDebug(false);
            cfg.setNumWorkers(12);
            cfg.setNumAckers(4);
            cfg.setMaxSpoutPending(2);
            cfg.setMessageTimeoutSecs(30);
            StormSubmitter.submitTopology(args[0], cfg, builder.createTopology());

        }else {
            //local debug mode
            cfg.setDebug(true);
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
