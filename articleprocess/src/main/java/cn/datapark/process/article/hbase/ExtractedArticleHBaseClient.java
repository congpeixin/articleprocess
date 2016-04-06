package cn.datapark.process.article.hbase;

import cn.datapark.process.article.config.ArticleExtractTopoConfig;
import cn.datapark.process.article.model.ArticlePage;
import cn.datapark.process.article.model.ArticleSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by eason on 16/1/20.
 */
public class ExtractedArticleHBaseClient extends HBaseClient {

    private static final Logger LOG = Logger.getLogger(ExtractedArticleHBaseClient.class);

    private String columnFamily = null;
    private String tableNanme = null;
    private String nameSpace = null;

    synchronized public void initClient(ArticleExtractTopoConfig config) {


        columnFamily = config.getExtractedHBaseTableColumnFamily();
        tableNanme = config.getExtractedHBaseArticleTableName();
        nameSpace = config.getExtractedHBaseNamespace();
        initClient(config.getExtractedHBaseZookeeperQuorum(),
                config.getExtractedHBaseZookeeperPort(),
                config.isExtractedHBaseClusterDisctributed());

    }

    public boolean putArticleSet(ArticleSet as) {


        List<Map<String, Object>> putList = new ArrayList<Map<String, Object>>();

        if (as.getSrcURL() == null) {
            LOG.error("failed to put article to HBase, article src url null!");
            return false;
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        //title
        if (as.getTitle() != null) {
            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_TITLE);
            putMap.put(CELL_VALUE, as.getTitle());
            putList.add(putMap);

        }

        if (as.getArticleSeenTime() != null) {
            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_ARTICLESEENTIME);
            putMap.put(CELL_VALUE, as.getArticleSeenTime());
            putList.add(putMap);

        }

        if (as.getAuthor() != null) {
            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_AUTHOR);
            putMap.put(CELL_VALUE, as.getAuthor());
            putList.add(putMap);
        }

        if (as.getPubTime() != null) {
            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_PUBTIME);
            putMap.put(CELL_VALUE, as.getPubTime());
            putList.add(putMap);
        }

        if (as.getSrcName() != null) {
            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_SRCNAME);
            putMap.put(CELL_VALUE, as.getSrcName());
            putList.add(putMap);

        }
        if (as.getSrcURL() != null) {

            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_SRCURL);
            putMap.put(CELL_VALUE, as.getSrcURL());
            putList.add(putMap);

        }

        if (as.getTags() != null) {

            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_TAGS);
            putMap.put(CELL_VALUE, as.getTags());
            putList.add(putMap);

        }

        if (as.getArticleCount() > 0) {

            HashMap<String, Object> putMap = new HashMap<String, Object>();
            putMap.put(ROWKEY, getRowKey(as.getSrcURL(), LocalDateTime.parse(as.getArticleSeenTime(), formatter)));
            putMap.put(COLUMN_FAMILY, columnFamily);
            putMap.put(COLUMN_NAME, ArticleSet.COLUMN_ALLPAGEARTICLE);
            putMap.put(CELL_VALUE, as.allArticleToJsonString());
            putList.add(putMap);
        }

        return batchPutMap(nameSpace, tableNanme, putList);


    }

    public static String IMG_STORE_LOCATION = "dp_sl";
    public static String IMG_STORE_ID = "dp_sid";

    public static String AVRO_IMG_STORE_LOCATION = "img_store_location";
    public static String AVRO_IMG_STORE_ID = "img_store_id";
    public static String AVRO_IMG_SRC_URL = "img_src_url";
    public static String AVRO_IMG_SIZE = "img_size";

    public void replaceIMGSrcUrl(String rowkeyUrl, Map<String, Map<String, String>> replaceImageUrlsMap, String seenTime) {

        //从hbase中取出对应的文章
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        byte[] bytes = getCellValueByRowkey(getRowKey(rowkeyUrl, LocalDateTime.parse(seenTime, formatter)), nameSpace, tableNanme, columnFamily, ArticleSet.COLUMN_ALLPAGEARTICLE);

        ArticleSet as = ArticleSet.buildFromJson(Bytes.toString(bytes));

        Set<String> keys = as.getKeySet();

        //遍历文章中的图片url,添加存储位置和存储id属性
        for (String key : keys) {
            ArticlePage ap = as.getArticle(key);
            Elements elements = ap.getContentElement().getElementsByTag("img");
            Iterator<Element> it = elements.iterator();
            while (it.hasNext()) {
                Element e = it.next();
                String rawSrcUrl = e.attr("src");
                Map relaceItemMap = replaceImageUrlsMap.get(rawSrcUrl);
                if (relaceItemMap != null) {
                    String storeID = (String) relaceItemMap.get(AVRO_IMG_STORE_ID);
                    if ((storeID != null) && (storeID.trim().length() > 0)) {
                        e.attr(IMG_STORE_ID,storeID);
                        e.attr(IMG_STORE_LOCATION, (String) relaceItemMap.get(AVRO_IMG_STORE_LOCATION));
                    }
                }
            }

            as.putArticle(ap);

        }
        //保存更新后的html内容到hbase中
        put(nameSpace,
                tableNanme,
                columnFamily,
                ArticleSet.COLUMN_ALLPAGEARTICLE,
                as.allArticleToJsonString(),
                getRowKey(rowkeyUrl, LocalDateTime.parse(seenTime, formatter)));

    }

    private String getRowKey(String urlString, LocalDateTime seenTime) {

        try {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            URL url = new URL(urlString);
            String rowkey = null;
            //String rowkey = url.getHost().substring(0,8) + ":" + seenTime.toEpochSecond(ZoneOffset.of("+08:00"))+ ":" + urlString;
            if (url.getHost().length() < 10) {
                rowkey = String.format("%-10s", url.getHost()).replace(" ", "-") + ":" + seenTime.format(formatter) + ":" + urlString;
            } else {
                rowkey = url.getHost().substring(0, 10) + seenTime.format(formatter) + urlString;
            }
            return rowkey;
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return null;
    }


    //相似URL list
    public static String COL_SIMILAR_URL_LIST = "sul";
    public static String COL_TARGET_SIMILAR_URL = "tsu";
    // 文章分词
    public static String COL_TARGET_SIMILAR_WORDS = "words";
    // 权重
    public static String COL_TARGET_SIMILAR_WEIGHT = "weight";

    /**
     * 将相似文章currentURL添加到hbase目标文章的相似url列表中
     *
     * @param currentURL 当前被判断相似的url
     * @param targetURL  需要更新的目标文章url
     */
    public void updateSimilarArticle(String currentURL, String targetURL, LocalDateTime currentURLSeentime) {

        byte[] byteRowkey = getRowkeybyScanCompareOp(".*" + targetURL, nameSpace, tableNanme);
        //更新相似文章sl字段(similarlist)
        if (byteRowkey == null) {
//            LOG.error("can not find row key by url:" + targetURL);
            return;
        }
        byte[] bytesSimilarURLList = getCellValueByRowkey(Bytes.toString(byteRowkey), nameSpace, tableNanme, columnFamily, COL_SIMILAR_URL_LIST);
        if (bytesSimilarURLList == null) {
            //第一个相似url
            if (!put(nameSpace,
                    tableNanme,
                    columnFamily,
                    COL_SIMILAR_URL_LIST,
                    currentURL,
                    Bytes.toString(byteRowkey))) {
//                LOG.error("failed to insert simalar article url to target. current url:" + currentURL + " target url:" + targetURL);
                return;

            }
        } else {
            //相似url添加到list末尾,逗号分割
            if (!put(nameSpace,
                    tableNanme,
                    columnFamily,
                    COL_SIMILAR_URL_LIST,
                    Bytes.toString(bytesSimilarURLList) + "," + currentURL,
                    Bytes.toString(byteRowkey))) {
//                LOG.error("failed to update simalar article url to target. current url:" + currentURL + " target url:" + targetURL);
                return;


            }
        }
        //以currentURL作为rowkey,targetURL作为value保存相似url
        put(nameSpace,
                tableNanme,
                columnFamily,
                COL_TARGET_SIMILAR_URL,
                targetURL,
                getRowKey(currentURL, currentURLSeentime));
        put(nameSpace,
                tableNanme,
                columnFamily,
                ArticleSet.COLUMN_SRCURL,
                currentURL,
                getRowKey(currentURL, currentURLSeentime));
    }

    /**
     *  将相似文章保存到hbase中 通过simhash 比较
     * @param currentURL
     * @param targetURL
     * @param currentURLSeentime
     * @param words  单词
     * @param weight    权重
     */
    public void updateSimilarArticle(String currentURL, String targetURL, LocalDateTime currentURLSeentime,String words,String weight) {

        byte[] byteRowkey = getRowkeybyScanCompareOp(".*" + targetURL, nameSpace, tableNanme);
        //更新相似文章sl字段(similarlist)
        if (byteRowkey == null) {
//            LOG.error("can not find row key by url:" + targetURL);
            return;
        }
        byte[] bytesSimilarURLList = getCellValueByRowkey(Bytes.toString(byteRowkey), nameSpace, tableNanme, columnFamily, COL_SIMILAR_URL_LIST);
        if (bytesSimilarURLList == null) {
            //第一个相似url
            if (!put(nameSpace,
                    tableNanme,
                    columnFamily,
                    COL_SIMILAR_URL_LIST,
                    currentURL,
                    Bytes.toString(byteRowkey))) {
//                LOG.error("failed to insert simalar article url to target. current url:" + currentURL + " target url:" + targetURL);
                return;

            }
        } else {
            //相似url添加到list末尾,逗号分割
            if (!put(nameSpace,
                    tableNanme,
                    columnFamily,
                    COL_SIMILAR_URL_LIST,
                    Bytes.toString(bytesSimilarURLList) + "," + currentURL,
                    Bytes.toString(byteRowkey))) {
//                LOG.error("failed to update simalar article url to target. current url:" + currentURL + " target url:" + targetURL);
                return;


            }
        }
        //以currentURL作为rowkey,targetURL作为value保存相似url
        put(nameSpace,
                tableNanme,
                columnFamily,
                COL_TARGET_SIMILAR_URL,
                targetURL,
                getRowKey(currentURL, currentURLSeentime));
        put(nameSpace,
                tableNanme,
                columnFamily,
                ArticleSet.COLUMN_SRCURL,
                currentURL,
                getRowKey(currentURL, currentURLSeentime));
        // 关键词
        put(nameSpace,
                tableNanme,
                columnFamily,
                COL_TARGET_SIMILAR_WORDS,
                words,
                getRowKey(currentURL, currentURLSeentime));
        //权重
        put(nameSpace,
                tableNanme,
                columnFamily,
                COL_TARGET_SIMILAR_WEIGHT,
                weight,
                getRowKey(currentURL, currentURLSeentime));
    }
    public void insertNewArticle(String currentURL, LocalDateTime currentURLSeentime,String words,String weight) {


        //以currentURL作为rowkey,targetURL作为value保存相似url
        put(nameSpace,
                tableNanme,
                columnFamily,
                ArticleSet.COLUMN_SRCURL,
                currentURL,
                getRowKey(currentURL, currentURLSeentime));
        // 关键词
        put(nameSpace,
                tableNanme,
                columnFamily,
                COL_TARGET_SIMILAR_WORDS,
                words,
                getRowKey(currentURL, currentURLSeentime));
        //权重
        put(nameSpace,
                tableNanme,
                columnFamily,
                COL_TARGET_SIMILAR_WEIGHT,
                weight,
                getRowKey(currentURL, currentURLSeentime));
    }

}
