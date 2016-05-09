package cn.datapark.process.article.hbase;

import cn.datapark.process.article.avro.decoder.ArticleSetAvroRecord;
import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.model.ArticleSet;
import cn.datapark.process.article.util.MD5Util;
import org.apache.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eason on 16/1/25.
 */
public class RawArticleHBaseClient extends ExtractedArticleHBaseClient {

    private static final Logger LOG = Logger.getLogger(ExtractedArticleHBaseClient.class);

    private String columnFamily = null;
    private String tableNanme = null;
    private String nameSpace = null;


    synchronized public void initClient(ArticleExtractTopoConfig config) {

        columnFamily = config.getRawHBaseTableColumnFamily();
        tableNanme = config.getRawHBaseArticleTableName();
        nameSpace = config.getRawHBaseNamespace();
        initClient(config.getRawHBaseZookeeperQuorum(),
                config.getRawHBaseZookeeperPort(),
                config.isRawHBaseClusterDisctributed());
    }


    public static final String SRC_URL = "src_url";
    public static final String SEEN_TIME = "seentime";
    public static final String SRC_NAME = "src_name";
    public static final String RAW_DATA = "rawdata";

    public boolean putRawArticle(ArticleSetAvroRecord asar) {


        List<Map<String, Object>> putList = new ArrayList<Map<String, Object>>();

        if (asar.getArticleSrcURL() == null) {
            LOG.error("failed to put raw article to HBase, article src url null!");
            return false;
        }

        //按照字段保存
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        try {

            if (asar.getArticleSrcURL() != null) {
                HashMap<String, Object> putMap = new HashMap<String, Object>();

                putMap.put(ROWKEY, getRowKey(asar.getArticleSrcURL(),
                        LocalDateTime.parse(asar.getArticleSeenTime(), formatter)));
                putMap.put(COLUMN_FAMILY, columnFamily);
                putMap.put(COLUMN_NAME, SRC_URL);
                putMap.put(CELL_VALUE, asar.getArticleSrcURL());
                putList.add(putMap);

            }

            if (asar.getArticleSeenTime() != null) {
                HashMap<String, Object> putMap = new HashMap<String, Object>();
                putMap.put(ROWKEY, getRowKey(asar.getArticleSrcURL(),
                        LocalDateTime.parse(asar.getArticleSeenTime(), formatter)));
                putMap.put(COLUMN_FAMILY, columnFamily);
                putMap.put(COLUMN_NAME, SEEN_TIME);
                putMap.put(CELL_VALUE, asar.getArticleSeenTime());
                putList.add(putMap);
            }

            if (asar.getArticleSrcName() != null) {
                HashMap<String, Object> putMap = new HashMap<String, Object>();
                putMap.put(ROWKEY, getRowKey(asar.getArticleSrcURL(),
                        LocalDateTime.parse(asar.getArticleSeenTime(), formatter)));
                putMap.put(COLUMN_FAMILY, columnFamily);
                putMap.put(COLUMN_NAME, SRC_NAME);
                putMap.put(CELL_VALUE, asar.getArticleSrcName());
                putList.add(putMap);
            }
            if (asar.getTags() != null) {

                HashMap<String, Object> putMap = new HashMap<String, Object>();
                putMap.put(ROWKEY, getRowKey(asar.getArticleSrcURL(),
                        LocalDateTime.parse(asar.getArticleSeenTime(), formatter)));
                putMap.put(COLUMN_FAMILY, columnFamily);
                putMap.put(COLUMN_NAME, ArticleSet.COLUMN_TAGS);
                putMap.put(CELL_VALUE, asar.getTags());
                putList.add(putMap);

            }

            if (asar.getArticleCount() != 0) {
                HashMap<String, Object> putMap = new HashMap<String, Object>();
                putMap.put(ROWKEY, getRowKey(asar.getArticleSrcURL(),
                        LocalDateTime.parse(asar.getArticleSeenTime(), formatter)));
                putMap.put(COLUMN_FAMILY, columnFamily);
                putMap.put(COLUMN_NAME, RAW_DATA);
                putMap.put(CELL_VALUE, asar.getArticleSetString());
                putList.add(putMap);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return batchPutMap(nameSpace, tableNanme, putList);

    }

    private String getRowKey(String urlString, LocalDateTime seenTime) {
        // 原来的row生成规则
/*
        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            URL url = new URL(urlString);
            String rowkey = null;
            //String rowkey = url.getHost().substring(0,8) + ":" + seenTime.toEpochSecond(ZoneOffset.of("+08:00"))+ ":" + urlString;
            if (url.getHost().length()<10){
                rowkey = String.format("%-10s",url.getHost()).replace(" ","-") + ":" + seenTime.format(formatter)+ ":" + urlString ;
            }else{
                rowkey = url.getHost().substring(0,10)  + seenTime.format(formatter) + urlString ;
            }
            return rowkey;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return null;*/

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            URL url = new URL(urlString);
            String rowkey = null;
            //String rowkey = url.getHost().substring(0,8) + ":" + seenTime.toEpochSecond(ZoneOffset.of("+08:00"))+ ":" + urlString;
            if (url.getHost().length() < 10) {
                rowkey = String.format("%-10s", url.getHost()).replace(" ", "-") + ":" + seenTime.format(formatter) + ":" + urlString;
            } else {
//                rowkey = url.getHost().substring(0, 10) + seenTime.format(formatter) + urlString;
                // 一级域名10位 不足0补齐 # time # url MD5 编码
                rowkey = addZeroForNum(urlString,10)+"#" + seenTime.format(formatter) +"#"+ MD5Util.getMD5Str(urlString) ;
            }
            return rowkey;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args){

        //String key = getRowKey("http://sina.com/asdfasfasf",LocalDateTime.of(2005,1,1,12,1,13));
        //System.out.println(key);
    }
}
