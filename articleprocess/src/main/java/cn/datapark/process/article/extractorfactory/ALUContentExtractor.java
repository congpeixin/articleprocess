package cn.datapark.process.article.extractorfactory;

import cn.datapark.process.article.model.ArticlePage;
import com.heidata.nlp.extactor.ALUExtractor;
import com.heidata.nlp.extactor.Article;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by eason on 16/1/13.
 */
public class ALUContentExtractor extends BaseExtractor {


    @Override
    public ArticlePage ExtractFromHTMLString(String htmlSource) {

        ArticlePage ap = new ArticlePage();
        ALUExtractor aluExtractor = new ALUExtractor();
        Article article = aluExtractor.ExtractFromHTMLString(htmlSource);
        ap.setTitle(article.getTitle());
        ap.setAuthor(article.getAuthor());


        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

        try {
            LocalDateTime.parse(article.getPubTime(), formatter);
            ap.setPubTime(article.getPubTime());
        } catch (Exception e) {
            formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            try {
                LocalDate.parse(article.getPubTime(), formatter);
                ap.setPubTime(article.getPubTime());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }


        ap.setContent(article.getContent());
        ap.setContentElement(article.getContentElement());
        ap.setScore(article.getScore());
        return ap;
    }


    public static void main(String arg[]) {
//        try {
//            ALUContentExtractor aluContentExtractor = new ALUContentExtractor();
//            BufferedReader br = new BufferedReader(new InputStreamReader(
//                    new FileInputStream(ALUContentExtractor.class.getClassLoader().getResource("sinanews.html").getPath())));
//            String htmlSource = "";
//
//            for (String line = br.readLine(); line != null; line = br.readLine()) {
//                htmlSource += line;
//            }
//            br.close();
//
//            ArticlePage ap = aluContentExtractor.ExtractFromHTMLString(htmlSource);
//            System.out.println(ap.getContentElement());
//            System.out.println(ap.getTitle());
//            System.out.println(ap.getPubTime());
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        String s = "2016-02-03";
        //s = "2011-10-10";
        //s= "1554-0-6 01:09:01";
        //s = "1256-1-6 23:23:00";
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        sdf.setLenient(false);
//        try {
//            sdf.parse(s);
//        } catch (ParseException e) {
//            sdf = new SimpleDateFormat("yyyy-MM-dd");
//            sdf.setLenient(false);
//            try {
//                Date d = sdf.parse(s);
//                System.out.println(d);
//            } catch (ParseException e1) {
//                e1.printStackTrace();
//            }
//        }




        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

        try {
            LocalDateTime.parse(s, formatter);
        } catch (Exception e) {
            formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            try {
                LocalDate.parse(s, formatter);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }


    }

}