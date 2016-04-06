package cn.datapark.process.article.extractorfactory;

import cn.datapark.process.article.model.ArticlePage;
import cn.edu.hfut.dmic.webcollector.net.HttpRequest;
import org.jsoup.nodes.Document;

/**
 * Created by eason on 16/1/12.
 */
public abstract class BaseExtractor implements IContentExtractor {

    protected String htmlSource = null;
    protected Document htmlDocument = null;
    protected String srcURL = null;

    public abstract ArticlePage ExtractFromHTMLString(String htmlSource);

    public ArticlePage ExtractFromURL(String URL){
        try{
            String html = fetchHtml(URL);
            ArticlePage ar = ExtractFromHTMLString(html);
            ar.setSrcURL(srcURL);
            return ar;
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    protected String fetchHtml(String URL) throws Exception {
        srcURL = URL;
        HttpRequest request = new HttpRequest(URL);
        String html = request.getResponse().getHtmlByCharsetDetect();
        return html;
    }
}
