package cn.datapark.process.article.extractorfactory;

import cn.datapark.process.article.model.ArticlePage;
import cn.edu.hfut.dmic.contentextractor.ContentExtractor;
import cn.edu.hfut.dmic.contentextractor.News;

/**
 * Created by eason on 16/1/12.
 */
public class WebCollectorExtractor extends BaseExtractor {

    @Override
    public ArticlePage ExtractFromHTMLString(String htmlSource) {
        try{
            News news = ContentExtractor.getNewsByHtml(htmlSource);
            ArticlePage ar = new ArticlePage();
            ar.setContent(news.getContent());
            ar.setContentElement(news.getContentElement());
            ar.setPubTime(news.getTime());
            ar.setTitle(news.getTitle());
            return ar;

        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

}
