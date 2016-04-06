package cn.datapark.process.article.bolts;

import cn.datapark.process.article.model.ArticlePage;
import cn.datapark.process.article.model.ArticleSet;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.Set;

/**
 * Created by eason on 16/1/20.
 */
public class ArticleSetSerializer extends Serializer<ArticleSet>  {

    @Override
    public void write(Kryo kryo, Output output, ArticleSet object) {

        output.writeString(object.getTitle());
        output.writeString(object.getAuthor());
        output.writeString(object.getPubTime());
        output.writeString(object.getTags());
        output.writeString(object.getSrcName());
        output.writeString(object.getSrcURL());
        output.writeString(object.getArticleSeenTime());
        output.writeInt(object.getArticleCount());
        Set<String> keys = object.getKeySet();
        for(String key:keys){
            ArticlePage ar =  object.getArticle(key);
            writeArticle(kryo,output,ar);
        }

    }

    private void writeArticle(Kryo kryo,Output output, ArticlePage ar){
        output.writeString(ar.getAuthor());
        output.writeString(ar.getContent());
        output.writeString(ar.getPubTime());
        output.writeString(ar.getSrcName());
        output.writeString(ar.getSrcURL());
        output.writeString(ar.getTitle());
        output.writeString(ar.getTags());
        output.writeInt(ar.getCurrentPageNumber());
        output.writeString(ar.getContentElement().html());

    }

    @Override
    public ArticleSet read(Kryo kryo, Input input, Class<ArticleSet> type) {
        ArticleSet as = new ArticleSet();
        as.setTitle(input.readString());
        as.setAuthor(input.readString());
        as.setPubTime(input.readString());
        as.setTags(input.readString());
        as.setSrcName(input.readString());
        as.setSrcURL(input.readString());
        as.setArticleSeenTime(input.readString());
        int count = input.readInt();
        for(int i=0;i<count;i++){
            as.putArticle(readArticle(kryo,input));
        }
        return  as;
    }

    private ArticlePage readArticle(Kryo kryo, Input input){
        ArticlePage ar = new ArticlePage();
        ar.setAuthor(input.readString());
        ar.setContent(input.readString());
        ar.setPubTime(input.readString());
        ar.setSrcName(input.readString());
        ar.setSrcURL(input.readString());
        ar.setTitle(input.readString());
        ar.setTags(input.readString());
        ar.setCurrentPageNumber(input.readInt());
        String html = input.readString();
        Document doc = Jsoup.parse(html);
        ar.setContentElement(doc);
        return  ar;
    }
}
