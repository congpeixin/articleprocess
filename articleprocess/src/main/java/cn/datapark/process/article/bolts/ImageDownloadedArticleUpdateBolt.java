package cn.datapark.process.article.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cn.datapark.process.article.avro.encoder.ImageDownloadRequestAvroEncoder;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.config.ConfigUtil;
import cn.datapark.process.article.es.ESClient;
import cn.datapark.process.article.es.ESUtil;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClient;
import cn.datapark.process.article.hbase.ExtractedArticleHBaseClientUtil;
import cn.datapark.process.article.util.GetInstanceException;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import cn.datapark.process.core.image.ConstantUtil;
import cn.datapark.process.core.image.ImageAlbumMeta;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.elasticsearch.action.update.UpdateResponse;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by eason on 16/1/22.
 */
public class ImageDownloadedArticleUpdateBolt extends BaseRichBolt {

    private static final Logger LOG = Logger.getLogger(ImageDownloadedArticleUpdateBolt.class);


    private OutputCollector collector;

    private ExtractedArticleHBaseClient hBaseClient;


    private static int THUMBNAILS_IMAGE_SIZE = 1024 * 10;

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

            hBaseClient = ExtractedArticleHBaseClientUtil.getClientInstance();
            hBaseClient.initClient(topoConfig);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void execute(Tuple tuple) {

        try {

            String imageAlbumType = tuple.getStringByField(ConstantUtil.StreamFields.ImageAlbumType);

            String docID = tuple.getStringByField(ConstantUtil.StreamFields.DocID);

            JSONObject imageAlbumJsonObject = new JSONObject(tuple.getStringByField(ConstantUtil.StreamFields.ImageAlbumMeta));

            //生成需要更新的图片存储位置信息
            Map<String, Map<String, String>> mapIMGUrls = new HashMap<String, Map<String, String>>();

            JSONArray jsonArray = imageAlbumJsonObject.getJSONArray("imageset");

            String imageStoreID = null;
            String imageStoreLocation = null;


            boolean isThumbnailsFound = false;
            for (int i = 0; i < jsonArray.length(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                Map<String, String> mapItem = new HashMap<String, String>();
                mapItem.put(ExtractedArticleHBaseClient.AVRO_IMG_STORE_ID,
                        jsonObject.getString(ExtractedArticleHBaseClient.AVRO_IMG_STORE_ID));
                mapItem.put(ExtractedArticleHBaseClient.AVRO_IMG_STORE_LOCATION,
                        jsonObject.getString(ExtractedArticleHBaseClient.AVRO_IMG_STORE_LOCATION));
                mapIMGUrls.put(jsonObject.getString(ExtractedArticleHBaseClient.AVRO_IMG_SRC_URL),
                        mapItem);
                //获取缩略图
                if (!isThumbnailsFound) {
                    long imageSize = jsonObject.getLong(ExtractedArticleHBaseClient.AVRO_IMG_SIZE);
                    //图片大于制定大小的第一张图为缩略图
                    if (imageSize > THUMBNAILS_IMAGE_SIZE) {
                        imageStoreID = jsonObject.getString(ExtractedArticleHBaseClient.AVRO_IMG_STORE_ID);
                        imageStoreLocation = jsonObject.getString(ExtractedArticleHBaseClient.AVRO_IMG_STORE_LOCATION);
                        isThumbnailsFound = true;
                    }

                }
            }

            //更新HBase中相应文章内的图片保存位置信息
            hBaseClient.replaceIMGSrcUrl(imageAlbumJsonObject.getString(ImageDownloadRequestAvroEncoder.SRC_ID), mapIMGUrls,
                    imageAlbumJsonObject.getString(ImageAlbumMeta.SeenTime));


            //更新ES中对应的索引信息,修改isimagereplaced 为true,并添加缩略图信息
            String indexType = "article";
            String indexName = ConfigUtil.getConfigInstance().getEsIndexName();
            updateArticleIndexSearchable(indexName, indexType, docID, true, imageStoreID, imageStoreLocation);
//            LOG.info("HBase data and Elasticsearch index updated,src_url:" + imageAlbumJsonObject.getString(ImageAlbumMeta.Src_URL));
            collector.ack(tuple);

        } catch (Exception e) {
//            LOG.error("fail to update imagedownloadrequest,ES docid:" + tuple.getStringByField(ConstantUtil.StreamFields.DocID));
            e.printStackTrace();
            collector.ack(tuple);
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();

        try {
            ESUtil.getESClientInstance().close();
            hBaseClient.close();
            DistributedConfigUtil.getInstance().destroy();
        } catch (GetInstanceException e) {
            e.printStackTrace();
        }
    }


    private void updateArticleIndexSearchable(String indexName, String indexType, String docID, boolean isimagereplaced, String imageStoreID, String imageStoreLocation) throws Exception {

        ESClient esClient = ESUtil.getESClientInstance();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("isimagereplaced", isimagereplaced);
        map.put("imagestoreid", imageStoreID);
        map.put("imagestorelocation", imageStoreLocation);
//            UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, docID)
//                    .doc(map);
//            esClient.getRawClient().update(updateRequest).get();

        UpdateResponse response = esClient.getRawClient().prepareUpdate(indexName, indexType, docID)
                .setDoc(map)
                .execute()
                .actionGet();


    }


}
