package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.datapark.process.article.avro.decoder.ArticleSetAvroDecoder;
import cn.datapark.process.article.avro.decoder.ArticleSetAvroDecoderUtil;
import cn.datapark.process.article.avro.decoder.ArticleSetAvroRecord;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.es.ESClient;
import cn.datapark.process.article.es.ESUtil;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClient;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClientUtil;
import cn.datapark.process.article.model.ArticleSet;
import cn.datapark.process.article.model.ImageDownloadRequest;
import cn.datapark.process.article.util.ConstantUtil;
import cn.datapark.process.article.util.HttpUtil;
import cn.datapark.process.article.util.TFIDFUtil;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import kafka.message.Message;
import net.sf.json.JSONObject;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by eason on 15/9/2.
 * <p>
 * ArticleContentExtractBolt 用于提取文章正文和标题
 * 生成3个Stream,分别存储提取后文章内容/存储原始内容/像kafka下载图片队列发送下载图片请求
 */
public class ArticleContentExtractBolt extends BaseRichBolt {

    private static final long serialVersionUID = 8620881782048248064L;

    private static final Logger LOG = Logger.getLogger(ArticleContentExtractBolt.class);

    private OutputCollector collector;

    private static final String ArticleKafkaAvrofile = "article_schema_kafka.avsc";
    private static final String ArticleSetKafkaAvrofile = "articleset_schema_kafka.avsc";

    //生成avro的schema
    public static String WriterSchemaName = "cn.datapark.avro.kafkaarticleset";
    //存储的schema
    public static String ReaderSchemaName = "cn.datapark.avro.kafkaarticleset";

    private boolean isAvroBinary = false;

    private ExtractedArticleHBaseClient hBaseClient;

    private HashMap<String, Integer> TDIDFWords;

    // 测试用文件日志


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();

        ArrayList<InputStream> alScehma = new ArrayList<InputStream>();

        //按子类到父类的顺序添加schema
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ArticleKafkaAvrofile));
        alScehma.add(getClass().getClassLoader().getResourceAsStream(ArticleSetKafkaAvrofile));

        ArticleSetAvroDecoder asab = ArticleSetAvroDecoderUtil.getInstance();

        asab.parseSchema(alScehma, WriterSchemaName, ReaderSchemaName);

        try {
            ConfigUtil.initConfig(ArticleContentExtractBolt.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
            ArticleExtractTopoConfig topoConfig = ConfigUtil.getConfigInstance();
            Configuration conf = new BaseConfiguration();
            conf.addProperty(ESClient.CLUSTER_NAME, topoConfig.getESClusterName());
            conf.addProperty(ESClient.CLUSTER_IP, topoConfig.getESClusterIP());
            conf.addProperty(ESClient.CLUSTER_PORT, topoConfig.getESClusterPort());
            ESUtil.initESClient(conf);

            hBaseClient = ExtractedArticleHBaseClientUtil.getClientInstance();
            hBaseClient.initClient(topoConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 初始化IDF词表
        TDIDFWords = new HashMap<String, Integer>();
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


            ESClient esClient = ESUtil.getESClientInstance();

            //搜索es判断文章是否已经存在
            String docid = ESClient.buildESDocIDFromURL(asar.getArticleSrcURL());
            GetResponse resp = esClient.getRawClient().prepareGet(ConfigUtil.getConfigInstance().getEsIndexName(),
                    ConfigUtil.getConfigInstance().getESIndexType(),
                    docid)
                    .execute()
                    .actionGet();
            if (resp.isExists()) {
                LOG.error("article exist url:" + asar.getArticleSrcURL() + " docid:" + docid);
                collector.ack(tuple);
                return;
            }

            ArticleSet as = ArticleSet.buidlArticleSetFromAvroRecord(asar);

            String simURL = null;
            simURL = checkSimilarArticle(as);
            if (simURL != null) {
                if (ConfigUtil.getConfigInstance().getSimilarAlgorithm() == 1) {
                    LOG.info("Similar article found, currentURL:" + as.getSrcURL() + " targetURL:" + simURL);
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    hBaseClient.updateSimilarArticle(as.getSrcURL(), simURL, LocalDateTime.parse(as.getArticleSeenTime(), formatter));
                }
                collector.ack(tuple);
                return;
            }


            ImageDownloadRequest idr = new ImageDownloadRequest();
            idr.build(as);

            if (idr.urlMap.size() == 0) {
                if (as.getAvePageSize() < 50) {
                    collector.ack(tuple);
                    return;
                } else {
                    collector.emit(ConstantUtil.Stream.EXTRACTED_ARTICLE, tuple, new Values(as));
                }
            } else {
                collector.emit(ConstantUtil.Stream.IMAGE_DOWNLOAD_REQUEST, tuple, new Values(idr));
                collector.emit(ConstantUtil.Stream.EXTRACTED_ARTICLE, tuple, new Values(as));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        collector.ack(tuple);

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declareStream(ConstantUtil.Stream.RAW_ARTICLE,
                new Fields(ConstantUtil.StreamFields.RAW_ARTICLE));
        outputFieldsDeclarer.declareStream(ConstantUtil.Stream.EXTRACTED_ARTICLE,
                new Fields(ConstantUtil.StreamFields.EXTRACTED_ARTICLESET));
        outputFieldsDeclarer.declareStream(ConstantUtil.Stream.IMAGE_DOWNLOAD_REQUEST,
                new Fields(ConstantUtil.StreamFields.ARTICLEIMAGEDOWNLOADREQUEST));
    }


    /**
     * 与已经抓取的文章对比,判断是否有相似文章
     *
     * @param as 当前抓取的文章
     * @return 已经抓取入库的相似文章url
     */
    private String checkSimilarArticle(ArticleSet as) {
        try {
            if (ConfigUtil.getConfigInstance().getSimilarAlgorithm() == 1) {
                return checkSimilarArticlebyES(as);
            } else if (ConfigUtil.getConfigInstance().getSimilarAlgorithm() == 2) {
                // 采用simhash 去重文章
                return checkSimilarArticlebySimHash(as);
            } else {
//                LOG.error("check similar algorithm type "+ConfigUtil.getConfigInstance().getSimilarAlgorithm()+" not support now");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 利用elasticsearch morelikethis实现相似文章的判断
     *
     * @param as
     * @return
     */
    private String checkSimilarArticlebyES(ArticleSet as) {
        String content = "";
        for (String key : as.getKeySet()) {
            content += as.getArticle(key).getContent();
        }

        try {
            QueryBuilder qb = QueryBuilders.moreLikeThisQuery(ConfigUtil.getConfigInstance().getSimilarESField())
                    .likeText(content);


            ESClient esClient = ESUtil.getESClientInstance();

            SearchResponse response = esClient.getRawClient().prepareSearch(ConfigUtil.getConfigInstance().getEsIndexName())
                    .setQuery(qb)
                    .setSize(1)
                    .execute().actionGet();

            if (response.getHits().getTotalHits() > 0) {
                if (response.getHits().getAt(0).getScore() > ConfigUtil.getConfigInstance().getSimilarESSCore()) {
                    return response.getHits().getAt(0).getSource().get(ArticleSet.COLUMN_SRCURL).toString();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 用simhash去重
     *
     * @param as
     * @return
     */
    private String checkSimilarArticlebySimHash(ArticleSet as) {
        long Start = System.currentTimeMillis();
        String simhashURL = "";
        try {
            ConfigUtil.initConfig(ArticleContentExtractBolt.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
            ArticleExtractTopoConfig topoConfig = ConfigUtil.getConfigInstance();
            simhashURL = topoConfig.getSimhashServerAddress();
        } catch (Exception e) {
            e.printStackTrace();
        }
        String content = "";
        for (String key : as.getKeySet()) {
            content += as.getArticle(key).getContent();
        }
        // 内容IK分词 算TF-IDF
        Map<String, String> wordsTFIDFValue = TFIDFUtil.countIDF(content);
        //请求接口 处理回调
        RequestModle requestModle = new RequestModle();
        requestModle.setUrl(as.getSrcURL());
        requestModle.setWeight(wordsTFIDFValue.get("idf"));
        requestModle.setWords(wordsTFIDFValue.get("words"));

        try {
            long httpStart = System.currentTimeMillis();
//            String response = HttpUtil.post("http://192.168.31.6:8080/simhashServer/v1/duplicateJudge/simhash", requestModle.toString());
//            String response = HttpUtil.post("http://localhost:8080/duplicateJudge/simhash", requestModle.toString());
            String response = HttpUtil.post(simhashURL, requestModle.toString());
            long httpEnd = System.currentTimeMillis();
//            LOG.info("simhash request time is " + (httpEnd - httpStart));
            JSONObject object = JSONObject.fromObject(response);
            String finger = object.getString("finger");
            String status = object.getString("status");
            String url = object.getString("url");
            if (!url.equals(as.getSrcURL())) {
                if ("EXIST".equalsIgnoreCase(status)) {
                    // simhash 值存在 有相近的值
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    hBaseClient.updateSimilarArticle(as.getSrcURL(), url, LocalDateTime.parse(as.getArticleSeenTime(), formatter), wordsTFIDFValue.get("words"), wordsTFIDFValue.get("idf"));
                    long End = System.currentTimeMillis();
                    LOG.info("Similar article found, currentURL:" + as.getSrcURL() + " targetURL:" + url + "  time is  " + (End - Start));
                    return url;
                } else {
                    // Hash值无重复 插入成功
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    hBaseClient.insertNewArticle(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter), wordsTFIDFValue.get("words"), wordsTFIDFValue.get("idf"));
                    long End = System.currentTimeMillis();
                    LOG.info("Article insert, currentURL:" + as.getSrcURL() + " time is " + (End - Start));
                    return null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.info("Similar article  error  happen URL:" + as.getSrcURL());
        }
        return null;
    }

}

class RequestModle {
    private String url;
    private String words;
    private String weight;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getWords() {
        return words;
    }

    public void setWords(String words) {
        this.words = words;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        // 因为接口那边参数比较怪异。。。。
        return "{\"func\":\"simhash\",\"requestData\":{ \"url\":\"" + url + '\"' +
                ", \"words\":\"" + words + '\"' +
                ", \"weight\":\"" + weight + '\"' +
                "}}";
    }
}
