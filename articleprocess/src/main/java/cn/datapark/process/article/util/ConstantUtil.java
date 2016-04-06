package cn.datapark.process.article.util;

/**
 * Created by eason on 16/1/13.
 */
public class ConstantUtil {
    public static class Stream{
        public static final String RAW_ARTICLE = "RawArticle";
        public static final String IMAGE_DOWNLOAD_REQUEST = "ImageDownloadRequest";
        public static final String EXTRACTED_ARTICLE = "ExtractedArticle";
        public static final String IMAGE_DOWNLOAD_REQUEST_TO_KAFKA = "ImageDownloadRequestToKafka";
        public static final String CRAWLED_IMAGE_ALBUM = "CrawledImageAlbum";
    }

    public static class StreamFields{
        public static final String RAW_ARTICLE = "ExtractedArticle";
        public static final String EXTRACTED_ARTICLESET = "ExtractedArticleSet";
        public static final String IMAGEURLMAP = "ImageURLMap";
        public static final String ARTICLESRCURL = "ArticleSrcURL";
        public static final String ARTICLEIMAGEDOWNLOADREQUEST = "ArticleImageDownloadRequest";
        public static final String KAFKABOLTKEY = "key";
        public static final String KAFKABOLTMESSAGE = "message";
        public static final String ImageAlbumMeta = "ImageAlbumMeta";
        public static final String IndexName = "IndexName";
        public static final String IndexType = "IndexType";
        public static final String DocID = "DocID";
        public static final String ImageAlbumType = "ImageAlbumType";
    }

    public static class Bolt {
        public static final String ARTICLECONTENT_EXTRACTBOLT = "ArticleContentExtractBolt";
        public static final String IMAGEDOWNLOAD_REQUESTBOLT = "ImageDownloadRequestBolt";
        public static final String EXTRACTEDCONTECT_STOREBOLT = "ExtractedContentStoreBolt";
        public static final String EXTRACTEDCONTECT_INDEXBOLT = "ExtractedContentIndexBolt";
        public static final String ARTICLESOURCE_KAFKASPOUT = "ArticleSourceKafkaSpout";
        public static final String IMAGEUPDATE_KAFKASPOUT = "ImageUpdateKafkaSpout";
        public static final String RAWAARTICLE_STOREBOLT = "RawArticleStoreBolt";

        public static final String KAFKABOLT = "KafkaBolt";
        public static final String CRAWLEDIMAGEALBUMAVROBOLT = "CrawledImageAlbumAvroBlot";
        public static final String IMAGEDOWNLOADEDARTICLEUPDATEBOLT = "ImageDownloadedArticleUpdateBolt";

    }
}
