package cn.datapark.process.article.avro.decoder;

import cn.datapark.process.article.model.RawArticle;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

/**
 * Created by eason on 16/1/12.
 */
public class ArticleSetAvroRecord{

    private GenericRecord articleSet = null;

    //对应avro schema
    private static final String ARTICLECOUNT =  "articlecount";
    private static final String ARTICLESRCURL = "src_url";
    private static final String ARTICLESRCNAME = "src_name";
    private static final String ARTICLESEENTIME = "seentime";
    private static final String ARTICLEPAGENUMBER =  "pagenumber";
    private static final String ARTICLECONTENT = "content";
    private static final String ARTICLESET = "articleset";
    private static final String ARTICLTAGS = "dp_tag";

    public GenericRecord getArticleSet() {
        return articleSet;
    }

    public void setArticleSet(GenericRecord articleSet) {
        this.articleSet = articleSet;
    }

    public int  getArticleCount(){
        return (Integer)articleSet.get(ARTICLECOUNT);
    }

    public String getArticleSrcURL(){
        return articleSet.get(ARTICLESRCURL).toString();
    }


    public  String getArticleSrcName() {
        return articleSet.get(ARTICLESRCNAME).toString();
    }

    public  String getArticleSeenTime() {
        return articleSet.get(ARTICLESEENTIME).toString();
    }


    public RawArticle getRawArticle(int i){

        GenericArray articleArray= (GenericArray)articleSet.get(ARTICLESET);
        GenericRecord articleRecord = (GenericRecord)articleArray.get(i);
        RawArticle rawArticle = new RawArticle();
        rawArticle.srcUrl = articleRecord.get(ARTICLESRCURL).toString();
        rawArticle.pageNum = (Integer) articleRecord.get(ARTICLEPAGENUMBER);
        rawArticle.content = articleRecord.get(ARTICLECONTENT).toString();
        return rawArticle;
    }

    public String getArticleSetString() {
        return articleSet.toString();
    }

    public String getTags(){
        return  articleSet.get(ARTICLTAGS).toString();
    }

}
