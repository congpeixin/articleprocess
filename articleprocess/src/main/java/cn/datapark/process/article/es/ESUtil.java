package cn.datapark.process.article.es;

import cn.datapark.process.article.util.GetInstanceException;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;


/**
 * Created by eason on 16/1/22.
 */
public class ESUtil {

    private static final Logger LOG = Logger.getLogger(ESUtil.class);

    private static ESClient esClientInstance = null;



    synchronized public static void initESClient(Configuration conf){

            if(esClientInstance != null){
                LOG.error("ESClient already init, should not init again");
                return;
            }

            esClientInstance = new ESClient();
            esClientInstance.init(conf.getString(ESClient.CLUSTER_NAME),
                    conf.getString(ESClient.CLUSTER_IP),
                    conf.getInt(ESClient.CLUSTER_PORT));

    }

    public static ESClient getESClientInstance() throws GetInstanceException {
        if(esClientInstance == null) {
            throw new GetInstanceException("Must call initESClient before getting instance");
        }

        return esClientInstance;

    }
}
