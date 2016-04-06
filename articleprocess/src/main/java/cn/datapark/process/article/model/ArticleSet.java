package cn.datapark.process.article.model;

import cn.datapark.process.article.avro.decoder.ArticleSetAvroRecord;
import cn.datapark.process.article.extractorfactory.BaseExtractor;
import cn.datapark.process.article.extractorfactory.ContentExtractorFactory;
import cn.datapark.process.article.extractorfactory.ContentExtractorType;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created by eason on 16/1/18.
 */
public class ArticleSet {

    private static final Logger LOG = Logger.getLogger(ArticleSet.class);

    private String srcURL;
    public static final String COLUMN_SRCURL = "src_url";

    private String srcName;
    public static final String COLUMN_SRCNAME = "src_name";

    private String articleSeenTime;
    public static final String COLUMN_ARTICLESEENTIME = "articleseentime";

    private String Title = null;
    public static final String COLUMN_TITLE = "title";

    private String Author = null;
    public static final String COLUMN_AUTHOR = "author";

    private String tags = null;
    public static final String COLUMN_TAGS = "tags";

    private String pubTime = null;
    public static final String COLUMN_PUBTIME = "pubtime";

    private HashMap<String, ArticlePage> articleHashMap = new HashMap<String, ArticlePage>();
    public static final String COLUMN_ALLPAGEARTICLE = "allpagearticle";


    public String getSrcURL() {
        return srcURL;
    }

    public void setSrcURL(String srcURL) {
        this.srcURL = srcURL;
    }

    public String getSrcName() {
        return srcName;
    }

    public void setSrcName(String srcName) {
        this.srcName = srcName;
    }

    public String getArticleSeenTime() {
        return articleSeenTime;
    }

    public void setArticleSeenTime(String articleSeenTime) {
        this.articleSeenTime = articleSeenTime;
    }

    public ArticlePage getArticle(String key) {
        return articleHashMap.get(key);
    }

    public void putArticle(ArticlePage ar) {
        articleHashMap.put(ar.getSrcURL(), ar);
    }

    public int getArticleCount() {
        return articleHashMap.size();
    }

    public Set<String> getKeySet() {
        return articleHashMap.keySet();
    }

    public String getTitle() {
        return Title;
    }

    public void setTitle(String title) {
        Title = title;
    }

    public String getAuthor() {
        return Author;
    }

    public void setAuthor(String author) {
        Author = author;
    }

    public String getPubTime() {
        return pubTime;
    }

    public void setPubTime(String pubTime) {
        this.pubTime = pubTime;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public HashMap<String, ArticlePage> getArticleHashMap() {
        return articleHashMap;
    }


    public static final String FIELD_PAGECOUNT = "pagecount";
    public static final String FIELD_CURRENTPAGENUMBER = "currentpagenumber";
    public static final String FIELD_CONTENT = "content";
    public static final String FIELD_CONTENTELEMENT = "contentElemet";
    public static final String FIELD_VERSION = "version";
    public static final int currentVersion = 1;

    public String allArticleToJsonString() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(FIELD_VERSION, 1);
        jsonObject.put(FIELD_PAGECOUNT, articleHashMap.size());
        Set<String> keys = articleHashMap.keySet();
        List<String> listArticles = new ArrayList<String>();
        for (String key : keys) {
            listArticles.add(articleHashMap.get(key).toJson());
        }
        jsonObject.put(COLUMN_ALLPAGEARTICLE, listArticles);
        return jsonObject.toString();
    }


    /**
     * 从Json字符串中生成分页文章集合
     *
     * @param json
     * @return
     */
    public static ArticleSet buildFromJson(String json) {
        ArticleSet as = new ArticleSet();

        JSONObject jsonObject = new JSONObject(json);

        try {
            int dataVersion = jsonObject.getInt(FIELD_VERSION);
            if (dataVersion != currentVersion) {
                LOG.info("only articlerset data version " + currentVersion + " supported");
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        int pageCount = jsonObject.getInt(FIELD_PAGECOUNT);
        JSONArray jsonArray = jsonObject.getJSONArray(COLUMN_ALLPAGEARTICLE);
        for (int i = 0; i < pageCount; i++) {
            ArticlePage ap = new ArticlePage();

            //JSONObject item = jsonArray.getJSONObject(i);
            String strItem = jsonArray.getString(i);
            JSONObject item = new JSONObject(strItem);

            try {
                ap.setAuthor(item.getString(COLUMN_AUTHOR));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                ap.setContent(item.getString(FIELD_CONTENT));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                Document doc = Jsoup.parseBodyFragment(item.getString(FIELD_CONTENTELEMENT));
                ap.setContentElement(doc.body());
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                ap.setCurrentPageNumber(item.getInt(FIELD_CURRENTPAGENUMBER));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                ap.setPubTime(item.getString(COLUMN_PUBTIME));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                ap.setSrcName(item.getString(COLUMN_SRCURL));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                ap.setSrcURL(item.getString(COLUMN_SRCURL));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            try {
                ap.setTitle(item.getString(COLUMN_TITLE));
            } catch (Exception e) {
                //e.printStackTrace();
            }

            try {
                ap.setTags(item.getString(COLUMN_TAGS));
            } catch (Exception e) {
                //e.printStackTrace();
            }
            as.putArticle(ap);
        }

        return as;
    }


    public static ArticleSet buidlArticleSetFromAvroRecord(ArticleSetAvroRecord asar) {

        ArticleSet as = new ArticleSet();

        try {
            as.setSrcURL(asar.getArticleSrcURL());
            as.setSrcName(asar.getArticleSrcName());
            as.setArticleSeenTime(asar.getArticleSeenTime());


            for (int i = 0; i < asar.getArticleCount(); i++) {
                RawArticle ra = asar.getRawArticle(i);
                ContentExtractorFactory cef = new ContentExtractorFactory();

                BaseExtractor extractor = cef.CreateContentExtractor(ContentExtractorType.TYPE_ALU);

                ArticlePage ar = extractor.ExtractFromHTMLString(ra.content);
                ar.setSrcURL(ra.srcUrl);
                ar.setCurrentPageNumber(ra.pageNum);

                LOG.info("src_url:" + ar.getSrcURL());
                if (ar.getPubTime() != null) {
                    LOG.info("pubtime:" + ar.getPubTime());
                    as.setPubTime(ar.getPubTime());
                }

                if (ar.getAuthor() != null) {
                    LOG.info("author:" + ar.getAuthor());
                    as.setAuthor(ar.getAuthor());
                }

                LOG.info(ar.getContent());

                if (ar.getTitle() != null) {
                    as.setTitle(ar.getTitle());
                }
                as.putArticle(ar);
                as.setTags(asar.getTags());
            }

        } catch (ContentExtractorFactory.ContentExtractorNotExistException e) {
            e.printStackTrace();
        }

        return as;
    }

    public long getAvePageSize(){
        String totalContent = "";
        for(String key:getKeySet()){
            ArticlePage ap = getArticle(key);
            totalContent  += ap.getContent();
        }
        return totalContent.length()/getArticleCount();
    }

}
