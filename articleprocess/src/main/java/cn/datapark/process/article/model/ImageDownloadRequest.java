package cn.datapark.process.article.model;

import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.*;

/**
 * Created by eason on 16/1/15.
 */
public class ImageDownloadRequest {

    public String srcUrl;
    public String srcName;
    public String seenTime;
    public HashMap<String,Set<String>> urlMap = new HashMap<String,Set<String>>();



    public ImageDownloadRequest(){

    }

    public void build(ArticleSet as){
        Set<String> keys = as.getKeySet();
        srcName = as.getSrcName();
        srcUrl = as.getSrcURL();
        seenTime = as.getArticleSeenTime();
        Iterator<String> it = keys.iterator();
        while(it.hasNext()){
            ArticlePage ar = as.getArticle(it.next());
            Element e = ar.getContentElement();
            Elements es = e.getElementsByTag("img");
            Set<String> urls = new HashSet<String>();
            for(Element element:es){
                urls.add(element.attr("src"));
            }
            urlMap.put(ar.getSrcURL(),urls);
        }
    }
}
