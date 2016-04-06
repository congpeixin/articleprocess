package cn.datapark.process.article.avro.encoder;

/**
 * Created by eason on 16/1/12.
 * AvroEncoder单例辅助类
 */
public class ImageDownloadRequestAvroEncoderUtil {

    private static ImageDownloadRequestAvroEncoder encoderInstance = null;

    public static synchronized ImageDownloadRequestAvroEncoder getInstance(){
        if(encoderInstance == null){
            encoderInstance = new ImageDownloadRequestAvroEncoder();
        }
        return encoderInstance;
    }
}
