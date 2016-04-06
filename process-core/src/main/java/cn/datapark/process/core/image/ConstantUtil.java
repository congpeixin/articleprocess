package cn.datapark.process.core.image;

/**
 * Created by eason on 15/9/2.
 */
public class ConstantUtil {
    public static class Bolt {
        public static final String KakfaSpout = "KafkaSpout";
        public static final String AvroBolt = "ImageAlbumAvroBolt";
        public static final String InsertBolt = "InsertESBolt";
        public static final String UpdateBolt = "UpdateBolt";
    }

    public static class Stream{
        public static final String NewImageAlbum = "stream-newimagealbum";
        public static final String ExistImageAlbum = "stream-existimagealbum";

    }

    public  static class StreamFields{
        public static final String ImageAlbumMeta = "ImageAlbumMeta";
        public static final String IndexName = "IndexName";
        public static final String IndexType = "IndexType";
        public static final String DocID = "DocID";
        public static final String ImageAlbumType = "ImageAlbumType";

    }
}
