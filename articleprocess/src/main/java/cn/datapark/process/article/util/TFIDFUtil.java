package cn.datapark.process.article.util;

import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.junit.Test;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by qiuwenyuan
 * Date 2016 / 03 /13:14
 * Version 1.0
 * Email  qiuwenyuan@zhongsou.com
 * QQ 908021504
 * Desc :
 */
public class TFIDFUtil {

    private static Map<String, Integer> doc = new HashMap<String, Integer>();

    static {
        try {
            String path = TFIDFUtil.class.getResource("/ik/IDFWords.txt").getPath();
            System.out.println(path);
            BufferedReader br = new BufferedReader(new FileReader(path));
            String r = br.readLine();
            while (r != null) {
                String[] split = r.split("\t");
                if ("null".equals(split[1])) {
                    doc.put(split[0], 0);
                } else {
                    doc.put(split[0], Integer.parseInt(split[1]));
                }
                r = br.readLine();
            }
//            System.out.println("------------  map init ok ------ ");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, String> countIDF(String content) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        HashMap<String, Integer> resTF = new HashMap<String, Integer>();
        // tfidf
        HashMap<String, Double> resTFValue = new HashMap<String, Double>();
        try {
            Double docNum = 3074687d + 3078758d + 3311594d +3300059d;

            IKAnalyzer analyzer = new IKAnalyzer();
            analyzer.setUseSmart(true);
            TokenStream tokenStream = analyzer.tokenStream("field", new StringReader(content));
            CharTermAttribute charTermAttribute = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            HashSet<String> hashSet = new HashSet<String>();
            Double count = 0.0;
            // resTF  单词以及出现次数
            while (tokenStream.incrementToken()) {
                String word = charTermAttribute.toString();
//                System.out.println(word);
                if (ShowChineseInUnicodeBlock.isContainChinese(word)){
                    if (resTF.get(word) == null) {
                        resTF.put(word, 1);
                    } else {
                        resTF.put(word, resTF.get(word) + 1);
                    }
                    count++;
                }else if (ShowChineseInUnicodeBlock.isEnglish(word)){
                    if (resTF.get(word) == null) {
                        resTF.put(word, 1);
                    } else {
                        resTF.put(word, resTF.get(word) + 1);
                    }
                    count++;
                }
            }

//            System.out.println("count is " + count);
            // 循环单词
            for (Map.Entry<String, Integer> tf : resTF.entrySet()) {
//            resTFValue.put(tf.getKey(),Float.intBitsToFloat(tf.getValue()/count));
                Double i = tf.getValue()/count;
                if (doc.get(tf.getKey()) != null){
//                    float value = (float) Math.log(docNum / (Float.intBitsToFloat(doc.get(tf.getKey())+1)));
                    Double number = Double.valueOf(doc.get(tf.getKey()));
                    Double value = (Double) Math.log(docNum /(number + 1.0d) );
                    resTFValue.put(tf.getKey(), value * i);
                }else {
                    Double value = (Double) Math.log(docNum);
                    resTFValue.put(tf.getKey(), value * i);
                }
            }
            StringBuffer words = new StringBuffer();
            StringBuffer idf = new StringBuffer();
            int mapSize = 1;
            for (Map.Entry<String, Double> tf : resTFValue.entrySet()){

                if (mapSize<  resTFValue.size()){
                    words.append(tf.getKey()+",");
                    idf.append(tf.getValue()+",");
                }else {
                    words.append(tf.getKey());
                    idf.append(tf.getValue());
                }
                mapSize ++;
            }
//            System.out.println("==============map size is :" +doc.size() );
//            System.out.println(words);
//            System.out.println(idf);
            result.put("words", words.toString());
            result.put("idf", idf.toString());
        }catch (Exception e){
            e.printStackTrace();
        }
        return result;
    }
    @Test
    public void  test1(){
//        String path = this.getClass().getClassLoader().getResource("/ik/sogou.dic").getPath();
//        URL path = TFIDFUtil.class.getResource("/ik/IDFWords.txt");
//        System.out.println(path);
        TFIDFUtil.countIDF("一个号码 啊啊啊 十");
//        TFIDFUtil.countIDF("啊啊啊图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 艾丽西卡·维坎德 (Alicia Vikander) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier 路易·威登LV (Louis Vuitton) 以“精神之旅”为主题的2016早春广告大片，由传奇摄影师Patrick Demarchelier拍摄完成。在路易·威登LV (Louis Vuitton) 恢弘的世界地图上，每个系列的旅行目的地都不同，这季创意总监尼古拉·盖斯奇埃尔 (Nicolas Ghesquiere) 将拍摄地选址于2016早春时装秀举办地——加州棕榈泉。 瑞典女演员艾丽西卡·维坎德 (Alicia Vikander) 继2015秋冬广告之后再一次同品牌缪斯米歇尔·威廉姆斯 (Michelle Williams) 出镜演绎，两位女演员身着2016早秋系列女装携最新手袋、 Capucines手提包、Twist链条包，在沙漠公路以及美丽的绿洲上开启全新一季的自由与冒险的旅程。图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 、艾丽西卡·维坎德 (Alicia Vikander) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 米歇尔·威廉姆斯 (Michelle Williams) 摄影师：Patrick Demarchelier图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 艾丽西卡·维坎德 (Alicia Vikander) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 艾丽西卡·维坎德 (Alicia Vikander) 摄影师：Patrick Demarchelier 图片来自Louis Vuitton 路易·威登LV (Louis Vuitton) 2016早春广告大片 模特： 艾丽西卡·维坎德 (Alicia Vikander) 摄影师：Patrick Demarchelier");
        boolean numeric = StringUtils.isNumeric("十");
        System.out.println(numeric);
    }
    @Test
    public void test2(){

        RequestModle requestModle = new RequestModle();
        requestModle.setUrl("wwww.baidu.com");
        requestModle.setWeight("0.21529243797198303,0.013107618211360576,0.03951569267666463,0.07834370193482021,0.034026088449703326,0.21529243797198303,0.04580837865462291,0.21529243797198303,0.21529243797198303,0.21529243797198303,0.057770268707034216,0.21529243797198303,0.03958942395142791,0.08002064768923069,0.0371247790465792,0.07723766545225458,0.013016171891637316,0.8611697518879321,0.048500768292118406,0.09732246706428767,0.06787255908494187,0.06770923099377522,0.0683024688819864,0.43058487594396605,0.0662078077958965,0.0356700281699218,0.10444769685925841,0.06275038745838438,0.09479991699526923,0.07908133168151935,0.21529243797198303,0.08975251747139082,0.015560314787966935,0.10420506805244066,0.21529243797198303,0.06677083105168935,0.21529243797198303,0.0709967450303898,0.43058487594396605,0.06487712804833227,0.21529243797198303,0.06424375972104092,0.0813104209461196,0.07437627296001861,0.43058487594396605,0.06966151689610665,0.0439443505979732,0.04831623000793661,0.083803080924379,0.07015501278496487,0.43058487594396605,0.06495758895780472,0.05670174944458362,0.10184146865794692,0.21529243797198303,0.11281597010557413,0.21529243797198303,0.21529243797198303,0.46031272233106973,0.05929181988424763,0.0802941820120646,0.21529243797198303,0.0803347828625701");
        requestModle.setWords("06,公司,英语,经纪公司,详情,kevin,所属,外,187cm,mama,身高,exo,语言,担当,韩国,出生地,更多,吴,工厂,双月,韩语,之夜,日出,exo-m,生于,职业,艺名,粤语,广东省,别名,个人特长,组合,查看,广州市,rapper,队长,s.m.entertainment,基本资料,1990年,生肖,sm,职务,天蝎座,中文名,kris,普通话,个人资料,星座,隶属于,门面,11月,文名,加拿大,代表作品,马,造星,6日,histor,亦凡,歌手,出生日期,y,国籍");

        Map<String,String> params = new HashMap<String,String>();
        params.put("url", requestModle.getUrl());
        params.put("words", requestModle.getWords());
        params.put("weight", requestModle.getWeight());

        String post = HttpUtil.post("http://192.168.31.6:8080/simhashServer/v1/duplicateJudge/simhash", requestModle.toString());
//        String response = HttpUtil.httpPost("http://192.168.31.6:8080/simhashServer/v1/duplicateJudge/simhash", params).toString();
//        System.out.printf(requestModle.toString());
        System.out.println(post);
        JSONObject object = JSONObject.fromObject(post);
        System.out.println(object.get("status"));
    }
}

class RequestModle{
    private  String url;
    private  String words;
    private  String weight;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getWords() {
        return words;
    }

    public void setWords(String words) {
        this.words = words;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "{\"func\":\"simhash\",\"requestData\":{ \"url\":\"" + url + '\"' +
                ", \"words\":\"" + words + '\"' +
                ", \"weight\":\"" + weight + '\"' +
                "}}";
    }
}