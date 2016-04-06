package cn.datapark.process.article.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by qiuwenyuan
 * Date 2016 / 03 /16:46
 * Version 1.0
 * Email  qiuwenyuan@zhongsou.com
 * QQ 908021504
 * Desc :
 */
public class ShowChineseInUnicodeBlock {
    public static void main(String[] args) {
//        String str = "我爱你，！？（）：；“”、。";
//        char[] charArray = str.toCharArray();
//        for (int i = 0; i < charArray.length; i++) {
//            isChinese(charArray[i]);
//        }
        String chinese = "中国god damn";
        System.out.println(isContainChinese(chinese));
        String english = " 十";
        System.out.println(isEnglish(english));
    }
    /**
     *
     * <p>
     * Title: isChinese
     * </p>
     * <p>
     * Description: 该函数就用来打印一些字符看看属于什么
     * </p>
     *
     * @param c
     *
     */
    public static void isChinese(char c) {
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS) {
            System.out.println(c + "--CJK_UNIFIED_IDEOGRAPHS");
        } else if (ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS) {
            System.out.println(c + "--CJK_COMPATIBILITY_IDEOGRAPHS");
        } else if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A) {
            // CJK Unified Ideographs Extension WikipediaUnicode扩展汉字
            // CJK Unified Ideographs Extension A 中日韩统一表意文字扩展区A ; 表意文字扩充A
            // CJK Unified Ideographs Extension B 中日韩统一表意文字扩展区B
            System.out.println(c + "--CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A");
        } else if (ub == Character.UnicodeBlock.GENERAL_PUNCTUATION) {// 通用标点
            System.out.println(c + "--GENERAL_PUNCTUATION");
        } else if (ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION) {
            System.out.println(c + "--CJK_SYMBOLS_AND_PUNCTUATION");
        } else if (ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {
            System.out.println(c + "--HALFWIDTH_AND_FULLWIDTH_FORMS");
        }
    }
    public static boolean isEnglish(String charaString) {
        return charaString.matches("^[a-zA-Z]*");
    }
    public static boolean isContainChinese(String str) {// 检测是否包含中文
        String regEx = "[\\u4E00-\\u9FA5]+";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        } else {
            return false;
        }
    }
}
