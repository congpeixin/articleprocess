package cn.datapark.process.article.extractorfactory;

import cn.datapark.process.article.model.ArticlePage;

/**
 * Created by eason on 16/1/12.
 */
public interface IContentExtractor {

    public ArticlePage ExtractFromHTMLString(String htmlSource);

    public ArticlePage ExtractFromURL(String URL);


}
