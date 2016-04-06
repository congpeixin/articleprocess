package cn.datapark.process.article.model;


import org.json.JSONObject;
import org.jsoup.nodes.Element;

/**
 * Created by eason on 15/10/8.
 */
public class ArticlePage {

    private String Title = null;
    private String Author = null;
    private String pubTime = null;
    private String Content = null;
    private String srcURL = null;
    private String srcName = null;
    private String tags = null;
    private double score = 0.0;
    private int currentPageNumber = 0;
    private Element contentElement = null;

    public String getSrcName() {
        return srcName;
    }

    public void setSrcName(String srcName) {
        this.srcName = srcName;
    }

    public String getSrcURL() {
        return srcURL;
    }

    public void setSrcURL(String srcURL) {
        this.srcURL = srcURL;
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

    public String getContent() {
        return Content;
    }

    public void setContent(String content) {
        Content = content;
    }

    public Element getContentElement() {
        return contentElement;
    }

    public void setContentElement(Element contentElement) {
        this.contentElement = contentElement;
    }

    public int getCurrentPageNumber() {
        return currentPageNumber;
    }

    public void setCurrentPageNumber(int currentPageNumber) {
        this.currentPageNumber = currentPageNumber;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String toJson() {

        JSONObject jsonObject = new JSONObject();
        if (Title != null)
            jsonObject.put(ArticleSet.COLUMN_TITLE, Title);
        if (Author != null)
            jsonObject.put(ArticleSet.COLUMN_AUTHOR, Author);
        if (pubTime != null)
            jsonObject.put(ArticleSet.COLUMN_PUBTIME, pubTime);
        if (srcURL != null)
            jsonObject.put(ArticleSet.COLUMN_SRCURL, srcURL);
        if (srcName != null)
            jsonObject.put(ArticleSet.COLUMN_SRCNAME, srcName);
        if(currentPageNumber==0){
            int a = 0;
        }
        jsonObject.put(ArticleSet.FIELD_CURRENTPAGENUMBER, currentPageNumber);
        if (Content != null)
            jsonObject.put(ArticleSet.FIELD_CONTENT, Content);
        if (contentElement != null)
            jsonObject.put(ArticleSet.FIELD_CONTENTELEMENT, contentElement.toString());
        if (tags != null)
            jsonObject.put(ArticleSet.COLUMN_TAGS, tags);

        return jsonObject.toString();

    }
}
