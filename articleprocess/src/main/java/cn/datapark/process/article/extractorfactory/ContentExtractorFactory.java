package cn.datapark.process.article.extractorfactory;

import cn.datapark.process.article.model.ArticlePage;

/**
 * Created by eason on 16/1/12.
 */
public class ContentExtractorFactory {

    public class ContentExtractorNotExistException extends Exception{
        ContentExtractorNotExistException(String message){
            super(message);
        }
    }

    public BaseExtractor CreateContentExtractor(String type) throws ContentExtractorNotExistException {

        if(type.equalsIgnoreCase(ContentExtractorType.TYPE_ALU)){
            return  new ALUContentExtractor();

        }else if(type.equalsIgnoreCase(ContentExtractorType.TYPE_WEBCOLLECTOR)){
            return new WebCollectorExtractor();
        }

        throw new ContentExtractorNotExistException("Not Support ContentExtractor Type:" + type);

    }

    public static void main(String arg[]){

        ContentExtractorFactory cef =  new ContentExtractorFactory();
        try {
            BaseExtractor extractor = cef.CreateContentExtractor(ContentExtractorType.TYPE_WEBCOLLECTOR);
            ArticlePage ar = extractor.ExtractFromURL("http://www.ifanr.com/606051");
            System.out.println(ar.getContentElement());

            System.out.println("==============================================================");
            System.out.println("==============================================================");
            System.out.println("==============================================================");
            System.out.println("==============================================================");

            extractor= cef.CreateContentExtractor(ContentExtractorType.TYPE_ALU);
            ar = extractor.ExtractFromURL("http://www.ifanr.com/606051");
            System.out.println(ar.getContentElement());

        } catch (ContentExtractorNotExistException e) {
            e.printStackTrace();
        }
    }
}
