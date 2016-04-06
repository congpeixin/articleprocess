package cn.datapark.process.core.utils;

import cn.datapark.process.core.config.configs.ESConfig;
import cn.datapark.process.core.config.utils.DistributedConfigUtil;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

/**
 * Created by eason on 15/8/30.
 */
public class ESUtil {

    private static Client esClientInstance = null;

    private static ESUtil instance = new ESUtil();

    private static final Logger LOG = Logger.getLogger(ESUtil.class);



    public Client getESInstance() {

        return esClientInstance;
    }

    /**
     * 初始化es client
     */
    private ESUtil() {
        try {
            //XMLConfiguration config = new XMLConfiguration(ESUtil.class.getClassLoader().getResource(ES_CONFIG_FILEPATH));
            //XMLConfiguration config = new XMLConfiguration((ES_CONFIG_FILEPATH));

            //从zookeeper中读取配置
            DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();
            ESConfig esConfig = dcu.getDistributedConfigCollection().getESConfig();


            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", esConfig.getClusterName())
                    .build();
            esClientInstance = new TransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(esConfig.getClusterIP(), esConfig.getClusterPort()));
        } catch ( Exception e) {
            e.printStackTrace();
        }

    }

    public static ESUtil getInstance() {

        return instance;
    }

    public void close() {

        esClientInstance.close();

    }

    /**
     * 创建es mapping
     * @param indexName 索引名称
     * @param mappingFile mapping文件名
     * @return 成功返回true，失败返回false
     */
    public boolean createMapping(String indexName,String mappingFile){

        try {
            //XMLConfiguration config = new XMLConfiguration(ESUtil.class.getClassLoader().getResource(ES_CONFIG_FILEPATH));
            //XMLConfiguration config = new XMLConfiguration((ES_CONFIG_FILEPATH));

            DistributedConfigUtil dcu = DistributedConfigUtil.getInstance();
            ESConfig esConfig = dcu.getDistributedConfigCollection().getESConfig();
            //String documentType = config.getString("index.type");

            InputStream is =
                    ESUtil.class.getClassLoader().getResourceAsStream(mappingFile);
            String jsonTxt = IOUtils.toString(is);


           // JSONObject json = new JSONObject(jsonTxt);
            /*
            final IndicesExistsResponse res = esClientInstance.admin().indices().prepareExists(indexName).execute().actionGet();
            if (res.isExists()) {
                LOG.info("index exists, updating mappings:"+ jsonTxt);
                esClientInstance.admin().indices().preparePutMapping(indexName)
                        .setSource(jsonTxt)
                        .execute()
                        .actionGet();

            }else{
            */
                LOG.info("creating mappings:" + jsonTxt);
                final CreateIndexRequestBuilder createIndexRequestBuilder = esClientInstance.admin().indices().prepareCreate(indexName);
                // MAPPING GOES HERE
                createIndexRequestBuilder.setSource(jsonTxt);
                // MAPPING DONE
                createIndexRequestBuilder.execute().actionGet();


        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (IndexAlreadyExistsException e){
            e.printStackTrace();
            return  false;
        }


        return true;
    }

    public static String formatDatetime(String srcDatetime){
        String dateTime = srcDatetime;

        dateTime = dateTime.trim();
        //将"/"转换成“-”
        dateTime = dateTime.replace("/","-");

        return dateTime;

    }


    /**
     * 通过URL创建es文档_id, _id为url的base64编码
     * @param URL
     * @return
     */
    public static String buildESDocIDFromURL(String URL){
        if((URL==null)||URL.trim().equals("")){
            return null;
        }
        try {
            final byte[] urlBytes = URL.getBytes("UTF-8");
            return Base64.getEncoder().encodeToString(urlBytes);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 通过索引名称和sku的源地址url判断sku是否存在
     * @param src_url
     * @param indexName
     * @return
     */
    public static boolean isSKUExistbyURL(String src_url,String indexName){
        QueryBuilder qb = QueryBuilders.termQuery("_id",buildESDocIDFromURL(src_url));
        SearchResponse resp = esClientInstance.prepareSearch(indexName)
                .setQuery(qb).execute().actionGet();
        return resp.getHits().getTotalHits()>0;
    }
}

