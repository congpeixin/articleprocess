package cn.datapark.process.article.config;

import cn.datapark.process.article.util.GetInstanceException;
import org.apache.log4j.Logger;

import java.io.InputStream;

/**
 * Created by eason on 16/1/15.
 */
public class ConfigUtil {
    private static final Logger LOG = Logger.getLogger(ConfigUtil.class);

    private static ArticleExtractTopoConfig configInstance = null;

    public static final String topoConfigfile = "articleprocess.properties";



    public static ArticleExtractTopoConfig getConfigInstance() throws GetInstanceException {
        if(configInstance == null) {
            throw new GetInstanceException("Must call initConfig before get instance");
        }
        return configInstance;
    }


    synchronized public static void initConfig(InputStream inputStream){
        if(configInstance == null){
            configInstance = new ArticleExtractTopoConfig();
            configInstance.load(inputStream);
        }else{
            LOG.error("Config already load, should not load again");
        }
    }

}
