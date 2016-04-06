package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClient;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClientUtil;
import cn.datapark.process.article.model.ArticleSet;
import cn.datapark.process.article.util.ConstantUtil;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by eason on 16/1/12.
 * 存储已经提取正文内容到Hbase中
 */
public class ExtractedContentStoreBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ExtractedContentStoreBolt.class);


    private OutputCollector collector;

    private ExtractedArticleHBaseClient hBaseClient;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();
        try {
            ConfigUtil.initConfig(ExtractedContentStoreBolt.class.getClassLoader().getResourceAsStream(ConfigUtil.topoConfigfile));
            ArticleExtractTopoConfig topoConfig = ConfigUtil.getConfigInstance();

            collector = outputCollector;
            hBaseClient = ExtractedArticleHBaseClientUtil.getClientInstance();
            hBaseClient.initClient(topoConfig);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {


        Object object = tuple.getValueByField(ConstantUtil.StreamFields.EXTRACTED_ARTICLESET);
        if(object instanceof ArticleSet){
            ArticleSet as = (ArticleSet)object;
            if(hBaseClient.putArticleSet(as)){
                collector.ack(tuple);
//                LOG.info("upsert HBase ok, src_url:"+ as.getSrcURL());
            }else {
                collector.ack(tuple);
//                LOG.info("failed to upsert HBase, url:"+ as.getSrcURL());

            }

        }else{
            collector.ack(tuple);
//            LOG.error("failed to get articleset from tuple");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
        hBaseClient.close();
        DistributedConfigUtil.getInstance().destroy();

    }
}