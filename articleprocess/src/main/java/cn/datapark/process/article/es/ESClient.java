package cn.datapark.process.article.es;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

/**
 * Created by eason on 16/1/22.
 * client for elastic search
 */
public class ESClient {

    private static final Logger LOG = Logger.getLogger(ESClient.class);

    private Client client = null;

    public static String CLUSTER_NAME = "es.cluster.name";
    public static String CLUSTER_IP = "es.cluster.ip";
    public static String CLUSTER_PORT = "es.cluster.port";


    synchronized public void init(String clusterName,String clusterIP,int clusterPort){

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName)
                .build();
        client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(clusterIP,clusterPort));
    }

    public Client getRawClient(){
        return client;
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
            return Base64.getUrlEncoder().encodeToString(urlBytes);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }
    public void close() {

        client.close();

    }

    /**
     * 创建es mapping
     * @param indexName 索引名称
     * @param mappingFile mapping文件名
     * @return 成功返回true，失败返回false
     */
    public boolean createMapping(String indexName,String mappingFile){

        try {

            InputStream is =
                    ESClient.class.getClassLoader().getResourceAsStream(mappingFile);
            String jsonTxt = IOUtils.toString(is);


            LOG.info("creating mappings:" + jsonTxt);
            final CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName);
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

}
