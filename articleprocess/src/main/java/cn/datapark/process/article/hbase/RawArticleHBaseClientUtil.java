package cn.datapark.process.article.hbase;

/**
 * Created by eason on 16/1/21.
 * 单例工具类
 */
public class RawArticleHBaseClientUtil {

    private static RawArticleHBaseClient clientInstance = new RawArticleHBaseClient();

    synchronized public static RawArticleHBaseClient getClientInstance(){
        return clientInstance;
    }
}
