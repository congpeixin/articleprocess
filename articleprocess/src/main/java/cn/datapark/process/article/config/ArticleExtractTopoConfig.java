package cn.datapark.process.article.config;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by eason on 16/1/12.
 */
public class ArticleExtractTopoConfig {

    private static final Logger LOG = Logger.getLogger(ArticleExtractTopoConfig.class);

    Properties prop = new Properties();

    private String ExtractorAlgorithm = null;


    private String extractedHBaseZookeeperQuorum = null;
    private String extractedHBaseZookeeperPort = null;
    private String isExtractedHBaseClusterDistributed = null;
    private String extractedHBaseNamespace = null;
    private String extractedHBaseArticleTableName = null;
    private String extractedHBaseTableColumnFamily = null;


    private String rawHBaseZookeeperQuorum = null;
    private String rawHBaseZookeeperPort = null;
    private String isRawHBaseClusterDistributed = null;
    private String rawHBaseNamespace = null;
    private String rawHBaseArticleTableName = null;
    private String rawHBaseTableColumnFamily = null;

    private String kafkaZookeeperServer;
    private String ArticleHtmlSourceTopic;
    private String ImageDownloadRequestTopic;
    private String kafkaZookeeperRoot;
    private long ConsumerMaxOffsetBehind;
    private boolean ConsumerUseStartOffsetTimeIfOffSetOutOfRange;
    private boolean ConsumerForceFromStart;
    private long ConsumerStartOffsetTime;

    private String imageRequestKafkaBrokerList = null;
    private String imageRequestKafkaRequiredAcks = null;

    private String esClusterName = null;
    private String esClusterIP = null;
    private String esClusterPort = null;
    private String esIndexName = null;
    private String esIndexType = null;
    private String esIndexMappingFileName = null;

    private int similarAlgorithm = 1;
    private String similarESField = null;
    private float similarESSCore = 2;

    private String imageAlbumMetaTopic = null;
    private String imageAlbunMetaConsumerRoot = null;
    private long imageAlbumMetaSpoutMaxOffsetBehind ;
    private boolean isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange;
    private boolean isImageAlbumMetaSpoutForceFromStart;
    private long imageAlbumMetaSpoutStartOffsetTime;


    synchronized public void load(InputStream configIn) {

        try {
            if (configIn != null) {
                prop.load(configIn);
            } else {
                LOG.error("input config stream null! ");
                return;
            }

            kafkaZookeeperServer = prop.getProperty("kafka.zookeeper.server");
            ArticleHtmlSourceTopic = prop.getProperty("kafka.article.topic");
            ImageDownloadRequestTopic = prop.getProperty("kafka.article.imagedownloadqueue.topic");
            kafkaZookeeperRoot = prop.getProperty("kafka.article.consumer.root");
            ConsumerMaxOffsetBehind = Long.parseLong(prop.getProperty("kafka.article.spout.maxoffsetbehind"));
            ConsumerUseStartOffsetTimeIfOffSetOutOfRange =
                    Boolean.parseBoolean(prop.getProperty("kafka.article.spout.usestartOffsettimeifoffsetoutofrange"));
            ConsumerForceFromStart = Boolean.parseBoolean(prop.getProperty("kafka.article.spout.forcefromstart"));
            ConsumerStartOffsetTime = Long.parseLong(prop.getProperty("kafka.article.spout.startoffsettime"));

            extractedHBaseZookeeperQuorum = prop.getProperty("hbase.extracted.zookeeper.quorum");
            extractedHBaseZookeeperPort = prop.getProperty("hbase.extracted.zookeeper.port");
            isExtractedHBaseClusterDistributed = prop.getProperty("hbase.extracted.zookeeper.isdistributed");
            extractedHBaseNamespace = prop.getProperty("hbase.extracted.namespace");
            extractedHBaseArticleTableName = prop.getProperty("hbase.extracted.article.tablename");
            extractedHBaseTableColumnFamily = prop.getProperty("hbase.extracted.article.table.columnfamily");


            rawHBaseZookeeperQuorum = prop.getProperty("hbase.raw.zookeeper.quorum");
            rawHBaseZookeeperPort = prop.getProperty("hbase.raw.zookeeper.port");
            isRawHBaseClusterDistributed = prop.getProperty("hbase.raw.zookeeper.isdistributed");
            rawHBaseNamespace = prop.getProperty("hbase.raw.namespace");
            rawHBaseArticleTableName = prop.getProperty("hbase.raw.article.tablename");
            rawHBaseTableColumnFamily = prop.getProperty("hbase.raw.article.table.columnfamily");

            esClusterName = prop.getProperty("es.cluster.name");
            esClusterIP = prop.getProperty("es.cluster.ip");
            esClusterPort = prop.getProperty("es.cluster.port");
            esIndexName = prop.getProperty("es.index.name");
            esIndexType = prop.getProperty("es.index.type");
            esIndexMappingFileName = prop.getProperty("es.index.mapping.file");

            //相似度计算参数
            similarAlgorithm = Integer.parseInt(prop.getProperty("similar.algorithm"));
            similarESField = prop.getProperty("similar.es.field");
            similarESSCore = Float.parseFloat(prop.getProperty("similar.es.score"));

            imageAlbumMetaTopic = prop.getProperty("kafka.imagealbummeta.topic");
            imageAlbunMetaConsumerRoot = prop.getProperty("kafka.imagealbummeta.consumer.root");
            imageAlbumMetaSpoutMaxOffsetBehind =
                    Long.parseLong(prop.getProperty("kafka.imagealbummeta.spout.maxoffsetbehind"));
            isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange =
                    Boolean.parseBoolean(prop.getProperty("kafka.imagealbummeta.spout.usestartOffsettimeifoffsetoutofrange"));
            isImageAlbumMetaSpoutForceFromStart =
                    Boolean.parseBoolean(prop.getProperty("kafka.imagealbummeta.spout.forcefromstart"));
            imageAlbumMetaSpoutStartOffsetTime =
                    Long.parseLong(prop.getProperty("kafka.imagealbummeta.spout.startoffsettime"));

            imageRequestKafkaBrokerList = prop.getProperty("metadata.broker.list");
            imageRequestKafkaRequiredAcks = prop.getProperty("request.required.acks");



        }catch (Exception e) {
            LOG.error("failed to load article topo config");
            e.printStackTrace();
        }

    }


    public Properties getProp() {
        return prop;
    }

    public void setProp(Properties prop) {
        this.prop = prop;
    }

    public String getExtractorAlgorithm() {
        return ExtractorAlgorithm;
    }

    public void setExtractorAlgorithm(String extractorAlgorithm) {
        ExtractorAlgorithm = extractorAlgorithm;
    }

    public String getKafkaZookeeperServer() {
        return kafkaZookeeperServer;
    }

    public void setKafkaZookeeperServer(String kafkaZookeeperServer) {
        this.kafkaZookeeperServer = kafkaZookeeperServer;
    }

    public String getArticleHtmlSourceTopic() {
        return ArticleHtmlSourceTopic;
    }

    public void setArticleHtmlSourceTopic(String articleHtmlSourceTopic) {
        ArticleHtmlSourceTopic = articleHtmlSourceTopic;
    }

    public String getImageDownloadRequestTopic() {
        return ImageDownloadRequestTopic;
    }

    public void setImageDownloadRequestTopic(String imageDownloadRequestTopic) {
        ImageDownloadRequestTopic = imageDownloadRequestTopic;
    }

    public String getKafkaZookeeperRoot() {
        return kafkaZookeeperRoot;
    }

    public void setKafkaZookeeperRoot(String kafkaZookeeperRoot) {
        this.kafkaZookeeperRoot = kafkaZookeeperRoot;
    }

    public long getConsumerMaxOffsetBehind() {
        return ConsumerMaxOffsetBehind;
    }

    public void setConsumerMaxOffsetBehind(long consumerMaxOffsetBehind) {
        ConsumerMaxOffsetBehind = consumerMaxOffsetBehind;
    }

    public boolean isConsumerUseStartOffsetTimeIfOffSetOutOfRange() {
        return ConsumerUseStartOffsetTimeIfOffSetOutOfRange;
    }

    public void setConsumerUseStartOffsetTimeIfOffSetOutOfRange(boolean consumerUseStartOffsetTimeIfOffSetOutOfRange) {
        ConsumerUseStartOffsetTimeIfOffSetOutOfRange = consumerUseStartOffsetTimeIfOffSetOutOfRange;
    }

    public boolean isConsumerForceFromStart() {
        return ConsumerForceFromStart;
    }

    public void setConsumerForceFromStart(boolean consumerForceFromStart) {
        ConsumerForceFromStart = consumerForceFromStart;
    }

    public long getConsumerStartOffsetTime() {
        return ConsumerStartOffsetTime;
    }

    public void setConsumerStartOffsetTime(long consumerStartOffsetTime) {
        ConsumerStartOffsetTime = consumerStartOffsetTime;
    }



    public String getESClusterName() {
        return esClusterName;
    }

    public void setESClusterName(String esClusterName) {
        this.esClusterName = esClusterName;
    }

    public String getESClusterIP() {
        return esClusterIP;
    }

    public void setESClusterIP(String esClusterIP) {
        this.esClusterIP = esClusterIP;
    }

    public String getESClusterPort() {
        return esClusterPort;
    }

    public void setESClusterPort(String esClusterPort) {
        this.esClusterPort = esClusterPort;
    }

    public String getEsIndexName() {
        return esIndexName;
    }

    public void setESIndexName(String esIndexName) {
        this.esIndexName = esIndexName;
    }

    public String getImageAlbumMetaTopic() {
        return imageAlbumMetaTopic;
    }

    public void setImageAlbumMetaTopic(String imageAlbumMetaTopic) {
        this.imageAlbumMetaTopic = imageAlbumMetaTopic;
    }

    public String getImageAlbunMetaConsumerRoot() {
        return imageAlbunMetaConsumerRoot;
    }

    public void setImageAlbunMetaConsumerRoot(String imageAlbunMetaConsumerRoot) {
        this.imageAlbunMetaConsumerRoot = imageAlbunMetaConsumerRoot;
    }

    public long getImageAlbumMetaSpoutMaxOffsetBehind() {
        return imageAlbumMetaSpoutMaxOffsetBehind;
    }

    public void setImageAlbumMetaSpoutMaxOffsetBehind(long imageAlbumMetaSpoutMaxOffsetBehind) {
        this.imageAlbumMetaSpoutMaxOffsetBehind = imageAlbumMetaSpoutMaxOffsetBehind;
    }

    public boolean isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange() {
        return isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange;
    }

    public void setImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange(boolean imageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange) {
        isImageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange = imageAlbumMetaSpoutUseStartOffsetTimeIfOffSetOutOfRange;
    }

    public boolean isImageAlbumMetaSpoutForceFromStart() {
        return isImageAlbumMetaSpoutForceFromStart;
    }

    public void setImageAlbumMetaSpoutForceFromStart(boolean imageAlbumMetaSpoutForceFromStart) {
        isImageAlbumMetaSpoutForceFromStart = imageAlbumMetaSpoutForceFromStart;
    }

    public long getImageAlbumMetaSpoutStartOffsetTime() {
        return imageAlbumMetaSpoutStartOffsetTime;
    }

    public void setImageAlbumMetaSpoutStartOffsetTime(long imageAlbumMetaSpoutStartOffsetTime) {
        this.imageAlbumMetaSpoutStartOffsetTime = imageAlbumMetaSpoutStartOffsetTime;
    }

    public String getExtractedHBaseZookeeperQuorum() {
        return extractedHBaseZookeeperQuorum;
    }

    public void setExtractedHBaseZookeeperQuorum(String extractedHBaseZookeeperQuorum) {
        this.extractedHBaseZookeeperQuorum = extractedHBaseZookeeperQuorum;
    }

    public String getExtractedHBaseZookeeperPort() {
        return extractedHBaseZookeeperPort;
    }

    public void setExtractedHBaseZookeeperPort(String extractedHBaseZookeeperPort) {
        this.extractedHBaseZookeeperPort = extractedHBaseZookeeperPort;
    }

    public String isExtractedHBaseClusterDisctributed() {
        return isExtractedHBaseClusterDistributed;
    }

    public void setIsExtractedHBaseClusterDistributed(String isExtractedHBaseClusterDistributed) {
        this.isExtractedHBaseClusterDistributed = isExtractedHBaseClusterDistributed;
    }

    public String getExtractedHBaseNamespace() {
        return extractedHBaseNamespace;
    }

    public void setExtractedHBaseNamespace(String extractedHBaseNamespace) {
        this.extractedHBaseNamespace = extractedHBaseNamespace;
    }

    public String getExtractedHBaseArticleTableName() {
        return extractedHBaseArticleTableName;
    }

    public void setExtractedHBaseArticleTableName(String extractedHBaseArticleTableName) {
        this.extractedHBaseArticleTableName = extractedHBaseArticleTableName;
    }

    public String getExtractedHBaseTableColumnFamily() {
        return extractedHBaseTableColumnFamily;
    }

    public void setExtractedHBaseTableColumnFamily(String extractedHBaseTableColumnFamily) {
        this.extractedHBaseTableColumnFamily = extractedHBaseTableColumnFamily;
    }

    public String getRawHBaseZookeeperQuorum() {
        return rawHBaseZookeeperQuorum;
    }

    public void setRawHBaseZookeeperQuorum(String rawHBaseZookeeperQuorum) {
        this.rawHBaseZookeeperQuorum = rawHBaseZookeeperQuorum;
    }

    public String getRawHBaseZookeeperPort() {
        return rawHBaseZookeeperPort;
    }

    public void setRawHBaseZookeeperPort(String rawHBaseZookeeperPort) {
        this.rawHBaseZookeeperPort = rawHBaseZookeeperPort;
    }

    public String isRawHBaseClusterDisctributed() {
        return isRawHBaseClusterDistributed;
    }

    public void setIsRawHBaseClusterDistributed(String isRawHBaseClusterDistributed) {
        this.isRawHBaseClusterDistributed = isRawHBaseClusterDistributed;
    }

    public String getRawHBaseNamespace() {
        return rawHBaseNamespace;
    }

    public void setRawHBaseNamespace(String rawHBaseNamespace) {
        this.rawHBaseNamespace = rawHBaseNamespace;
    }

    public String getRawHBaseArticleTableName() {
        return rawHBaseArticleTableName;
    }

    public void setRawHBaseArticleTableName(String rawHBaseArticleTableName) {
        this.rawHBaseArticleTableName = rawHBaseArticleTableName;
    }

    public String getRawHBaseTableColumnFamily() {
        return rawHBaseTableColumnFamily;
    }

    public void setRawHBaseTableColumnFamily(String rawHBaseTableColumnFamily) {
        this.rawHBaseTableColumnFamily = rawHBaseTableColumnFamily;
    }

    public String getImageRequestKafkaRequiredAcks() {
        return imageRequestKafkaRequiredAcks;
    }

    public void setImageRequestKafkaRequiredAcks(String imageRequestKafkaRequiredAcks) {
        this.imageRequestKafkaRequiredAcks = imageRequestKafkaRequiredAcks;
    }

    public String getImageRequestKafkaBrokerList() {
        return imageRequestKafkaBrokerList;
    }

    public void setImageRequestKafkaBrokerList(String imageRequestKafkaBrokerList) {
        this.imageRequestKafkaBrokerList = imageRequestKafkaBrokerList;
    }

    public String getESIndexMappingFileName(){
        return esIndexMappingFileName;
    }

    public String getESIndexType(){
        return esIndexType;
    }

    public String getSimilarESField(){
        return similarESField;
    }

    public int getSimilarAlgorithm() {
        return similarAlgorithm;
    }

    public float getSimilarESSCore() {
        return similarESSCore;
    }
}
