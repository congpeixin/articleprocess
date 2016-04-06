package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.es.ESClient;
import cn.datapark.process.article.es.ESUtil;
import cn.datapark.process.article.model.ArticleSet;
import cn.datapark.process.article.model.ImageDownloadRequest;
import cn.datapark.process.article.util.ConstantUtil;
import cn.datapark.process.article.util.GetInstanceException;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by eason on 16/1/22.
 */
public class ExtractedContentIndexBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ExtractedContentIndexBolt.class);

    private OutputCollector collector;

    private volatile int sucCreateCount = 0 ;
    private volatile int failedCreateCount = 0 ;

    private static int articleSizeThreshold =250;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        collector = outputCollector;

        DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();
        try {
            ConfigUtil.initConfig(ExtractedContentStoreBolt.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
            ArticleExtractTopoConfig topoConfig = ConfigUtil.getConfigInstance();
            Configuration conf = new BaseConfiguration();
            conf.addProperty(ESClient.CLUSTER_NAME, topoConfig.getESClusterName());
            conf.addProperty(ESClient.CLUSTER_IP, topoConfig.getESClusterIP());
            conf.addProperty(ESClient.CLUSTER_PORT, topoConfig.getESClusterPort());
            ESUtil.initESClient(conf);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {


        try {

            Object object = tuple.getValueByField(ConstantUtil.StreamFields.EXTRACTED_ARTICLESET);
            ArticleSet as;
            if (object instanceof ArticleSet) {
                 as = (ArticleSet) object;
            } else {
//                LOG.error("failed to get articleset from tuple");
                collector.ack(tuple);
                return;
            }

            ESClient esClient = ESUtil.getESClientInstance();


            String indexType = "article";
            String indexName = ConfigUtil.getConfigInstance().getEsIndexName();
            if (indexName == null) {
                collector.ack(tuple);
//                LOG.warn("failed to get index name and index type");
                return;
            }
            Map map;

            ImageDownloadRequest idr = new ImageDownloadRequest();
            idr.build(as);

            //正文中没有图片,且正文字数小于articleSizeThreshold,则不索引文章内容
            if(idr.urlMap.size()==0&&as.getAvePageSize()<articleSizeThreshold){
                collector.ack(tuple);
                return;
            }else if(idr.urlMap.size()==0){
                map = buildESSource(as,false);
            }else{
                map = buildESSource(as,true);

            }

            IndexResponse response = esClient.getRawClient().prepareIndex(indexName, indexType)
                    .setId(ESClient.buildESDocIDFromURL(as.getSrcURL()))
                    .setSource(map)
                    .execute()
                    .actionGet();

            String _index = response.getIndex();
            String _type = response.getType();
            String _id = response.getId();
            long _version = response.getVersion();
//            LOG.info("New extracted article index:" + _index + "type:" + _type + "id:" + _id + "version:" + Long.toString(_version));

            boolean created = response.isCreated();
            if (created) {
                collector.ack(tuple);
//                LOG.info("extracted article Index Succeed Count:" + Integer.toString(sucCreateCount++));
            } else {
                collector.ack(tuple);
//                LOG.info("extracted article Index Failed Count:" + Integer.toString(failedCreateCount++));
            }

        } catch (Exception e) {

//            LOG.error("failed to index extracted article, Count:" + Integer.toString(failedCreateCount++));
            collector.ack(tuple);
            e.printStackTrace();
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();

        try {
            ESUtil.getESClientInstance().close();
            DistributedConfigUtil.getInstance().destroy();
        } catch (GetInstanceException e) {
            e.printStackTrace();
        }

    }

    private Map<String,Object> buildESSource(ArticleSet as,boolean isImageNeeded){

        Map<String,Object> sourceMap = new HashMap<String, Object>();
        sourceMap.put("isimagereplaced",!isImageNeeded);
        sourceMap.put(ArticleSet.COLUMN_SRCURL,as.getSrcURL());
        sourceMap.put(ArticleSet.COLUMN_SRCNAME,as.getSrcName());
        sourceMap.put(ArticleSet.COLUMN_ARTICLESEENTIME,as.getArticleSeenTime());
        sourceMap.put(ArticleSet.COLUMN_TITLE,as.getTitle());
        if(as.getTags()!=null)
            sourceMap.put(ArticleSet.COLUMN_TAGS,as.getTags());
        if(as.getPubTime()!=null){
            sourceMap.put(ArticleSet.COLUMN_PUBTIME,as.getPubTime());
        }
        if(as.getAuthor()!=null){
            sourceMap.put(ArticleSet.COLUMN_AUTHOR,as.getAuthor());
        }
        if(as.getArticleCount()>0){
            String content = "";
            for(String key:as.getKeySet()){
                content += as.getArticle(key).getContent();
            }
            sourceMap.put(ArticleSet.FIELD_CONTENT,content);
        }


        return sourceMap;
    }
}
