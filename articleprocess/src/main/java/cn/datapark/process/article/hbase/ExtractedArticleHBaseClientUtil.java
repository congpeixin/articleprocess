package cn.datapark.process.article.hbase;

/**
 * Created by eason on 16/1/21.
 * 单例工具类
 */
public class ExtractedArticleHBaseClientUtil {

    private static ExtractedArticleHBaseClient clientInstance = new ExtractedArticleHBaseClient();

    synchronized public static ExtractedArticleHBaseClient getClientInstance(){
        return clientInstance;
    }
}
