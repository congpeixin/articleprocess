package cn.datapark.process.article.avro.decoder;

/**
 * Created by eason on 16/1/12.
 * ArticleSetAvroDecoder单例辅助类
 */
public class ArticleSetAvroDecoderUtil {

    private static ArticleSetAvroDecoder builderInstance = null;

    public static synchronized ArticleSetAvroDecoder getInstance(){
        if(builderInstance == null){
            builderInstance = new ArticleSetAvroDecoder();
        }
        return builderInstance;
    }
}
