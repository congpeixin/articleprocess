package cn.datapark.process.article.avro.encoder;

import cn.datapark.process.article.model.ImageDownloadRequest;
import org.apache.avro.generic.GenericRecord;

import java.util.*;

/**
 * Created by eason on 16/1/18.
 * 处理图片下载请求,生成avro record以及对应的文本
 */
public class ImageDownloadRequestAvroEncoder extends AvroEncoder {

    //对应avro  schema中的字段名称
    public static final String SRC_URL = "src_url";
    public static final String SRC_ID = "src_id";
    public static final String COUNT = "count";
    public static final String IMAGESET = "imageset";
    public static final String TYPE = "type";
    public static final String TYPE_VALUE = "articleset";
    public static final String IMAGE_SRC_URL = "img_src_url";
    public static final String SRC_NAME = "src_name";
    public static final String SEENTIME = "seentime";



    /**
     * 将ImageDownloadRequest转换成基类处理的Map
     * @param imageDownloadRequest
     * @return
     */
    public Map<String,String> build(ImageDownloadRequest imageDownloadRequest){

        Set<String> keys = imageDownloadRequest.urlMap.keySet();
        HashMap<String,String> result = new HashMap<String, String>();
        for(String key:keys){
            Set<String> urls = imageDownloadRequest.urlMap.get(key);
            if(urls.size()>0){
                Map avroMap = buildAvroMap(imageDownloadRequest.srcName,imageDownloadRequest.seenTime,key,imageDownloadRequest.srcUrl,urls);
                GenericRecord record = build(avroMap);
                result.put(key,encodeString(record));
            }

        }
        return result;
    }

    /**
     * 将多页文章的文章集拆分,每一页文章中需要下载的图片作为一个图片下载集合
     * @param srcUrl
     * @param urls
     * @return
     */
    private Map<String,Object> buildAvroMap(String srcName,String seentime,String srcUrl,String srcID,Set<String> urls){
        HashMap<String,Object> avroMap = new HashMap<String,Object>();
        avroMap.put(SRC_NAME,srcName);
        avroMap.put(SRC_URL,srcUrl);
        avroMap.put(SEENTIME,seentime);
        avroMap.put(COUNT,urls.size());
        List<Map<String,Object>> imageSet = new ArrayList<Map<String, Object>>();
        for(String url:urls){
            imageSet.add(buildImageMetaMap(url));
        }
        avroMap.put(IMAGESET,imageSet);
        avroMap.put(TYPE,TYPE_VALUE);
        avroMap.put(SRC_ID,srcID);

        return  avroMap;
    }

    /**
     * 生图片集中的单个图片下载请求
     * @param url
     * @return
     */
    private  Map<String,Object> buildImageMetaMap(String url){

        Map<String,Object> imageMetaMap = new HashMap<String, Object>();
        if(url.trim().length()>0)
            imageMetaMap.put(IMAGE_SRC_URL,url);
        return imageMetaMap;
    }


    public static void main(String arg[]){
        ImageDownloadRequestAvroEncoder imageDownloadRequestAvroEncoder = new ImageDownloadRequestAvroEncoder();
    }
}
