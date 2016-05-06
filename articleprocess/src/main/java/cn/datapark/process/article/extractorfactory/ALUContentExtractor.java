package cn.datapark.process.article.extractorfactory;

import cn.datapark.process.article.model.ArticlePage;
import com.heidata.nlp.extactor.ALUExtractor;
import com.heidata.nlp.extactor.Article;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

//import java.time.LocalDate;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;

/**
 * Created by eason on 16/1/13.
 */
public class ALUContentExtractor extends BaseExtractor {


    @Override
    public ArticlePage ExtractFromHTMLString(String htmlSource) {
        // master zhou code resource
      /*  ArticlePage ap = new ArticlePage();
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
        return ap;*/

        ArticlePage ap = new ArticlePage();
        ALUExtractor aluExtractor = new ALUExtractor();
        Article article = aluExtractor.ExtractFromHTMLString(htmlSource);
        ap.setTitle(article.getTitle());
        ap.setAuthor(article.getAuthor());


//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");


        try {
//            LocalDateTime.parse(article.getPubTime(), formatter);
            DateTime dateTime2 = DateTime.parse(article.getPubTime(), format);
            ap.setPubTime(article.getPubTime());
        } catch (Exception e) {
            format = DateTimeFormat.forPattern("yyyy/MM/dd");
            try {
                DateTime.parse(article.getPubTime(), format);
                ap.setPubTime(article.getPubTime());
            } catch (Exception e1) {
                // 没办法处理时间
                ap.setPubTime(article.getPubTime());
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

        String s = "2119/05/30 07:00:00";
//        s = "2011-10-10 07:00:00";
        //s= "1554-0-6 01:09:01";
        s = "1256-01-06 23:23:00";
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




//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
////        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d MMM uuuu");
//
//        try {
//            LocalDateTime.parse(s, formatter);
//        } catch (Exception e) {
//            formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
//            try {
//                LocalDate.parse(s, formatter);
//                System.out.println(s);
//            } catch (Exception e1) {
//                e1.printStackTrace();
//            }
//        }


        // JoDa 处理
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss");
        try {
            DateTime.parse(s, format);
        }catch (Exception e){
            try {
                format = DateTimeFormat.forPattern("yyyy/MM/dd");
                DateTime.parse(s, format);
            }catch (Exception e2){
                System.out.println("can not handle this time");
//                e2.printStackTrace();
            }

        }
//        DateTimeFormatter format = DateTimeFormat .forPattern("yyyy-MM-dd HH:mm:ss");
//        //时间解析
//        DateTime dateTime2 = DateTime.parse("2012-2-31 23:22:45", format);
//
//        //时间格式化，输出==> 2012/12/21 23:22:45 Fri
//        String string_u = dateTime2.toString("yyyy/MM/dd HH:mm:ss EE");
//        System.out.println(string_u);
    }

}