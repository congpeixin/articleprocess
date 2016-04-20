package cn.datapark.process.article.util;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
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
//            String path = TFIDFUtil.class.getResource("/ik/IDFWords.txt").getPath(); 不带智能分词 库
            // 不打jar包的情况下 读取词典的方式不一样
//            String path = TFIDFUtil.class.getResource("/ik/SogouIDF.txt").getPath(); 带智能 词库
            // 打jar 包情况下读取文件
            InputStream inputStream = TFIDFUtil.class.getClassLoader().getResourceAsStream("ik/SogouIDF.txt");
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Map<String, String> countIDF(String content) {
        Map<String, String> result = new LinkedHashMap<String, String>();
        HashMap<String, Integer> resTF = new HashMap<String, Integer>();
        // tfidf
        HashMap<String, Double> resTFValue = new HashMap<String, Double>();
        System.out.println("初始化的大小"+doc.size());
        try {
//            Double docNum = 3074687d + 3078758d + 3311594d +3300059d;   IDFWords 语料库的大小
            Double docNum = 187355929d;

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
                if (ShowChineseInUnicodeBlock.isContainChinese(word)) {
                    if (resTF.get(word) == null) {
                        resTF.put(word, 1);
                    } else {
                        resTF.put(word, resTF.get(word) + 1);
                    }
                    count++;
                } else if (ShowChineseInUnicodeBlock.isEnglish(word)) {
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
                Double i = tf.getValue() / count;
                if (doc.get(tf.getKey()) != null) {
//                    float value = (float) Math.log(docNum / (Float.intBitsToFloat(doc.get(tf.getKey())+1)));
                    Double number = Double.valueOf(doc.get(tf.getKey()));
                    Double value = Math.log(docNum / (number + 1.0d));
                    resTFValue.put(tf.getKey(), value * i);
                } else {
                    Double value = Math.log(docNum);
                    resTFValue.put(tf.getKey(), value * i);
                }
            }
            StringBuffer words = new StringBuffer();
            StringBuffer idf = new StringBuffer();
            StringBuffer tfs = new StringBuffer();
            int mapSize = 1;
            for (Map.Entry<String, Double> tf : resTFValue.entrySet()) {

                if (mapSize < resTFValue.size()) {
                    words.append(tf.getKey() + ",");
                    idf.append(tf.getValue() + ",");
                    tfs.append(resTF.get(tf.getKey()) + ",");
                } else {
                    words.append(tf.getKey());
                    idf.append(tf.getValue());
                    tfs.append(resTF.get(tf.getKey()));
                }
                mapSize++;
            }
            int tfsSize = 1 ;
//            System.out.println("==============map size is :" +doc.size() );
//            System.out.println(words);
//            System.out.println(idf);
            result.put("words", words.toString());
            result.put("idf", idf.toString());
            // 计算LDA 预留 字段
            result.put("tfs", tfs.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

class RequestModle {
    private String url;
    private String words;
    private String weight;

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