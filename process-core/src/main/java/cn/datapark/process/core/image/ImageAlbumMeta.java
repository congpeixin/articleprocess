package cn.datapark.process.core.image;

import org.apache.avro.generic.GenericRecord;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Iterator;

/**
 * Created by eason on 15/10/9.
 */
public class ImageAlbumMeta extends HashSet{


    public static String Src_URL = "src_url";
    public static String Category = "category";
    public static String ID = "id";
    public static String SeenTime = "seentime";
    public static String Type = "type";
    public static String SRC_ID ="src_id";
    private GenericRecord IAMRecord = null;


    /*
    private String ID =null;
    private String SrcName = null;
    private String SrcURL = null;
    private String Title = null;
    private String Desc = null;
    private HashSet<String> Tags = new HashSet<String>();
    private HashSet<String> Authors = new HashSet<String>();
    private CoverImage Cover = null;
    private Date SeenTime = null;
    private Date SrcCreateTime = null;
    private Date ScrUpdateTime = null;
    private HashSet<ImageMeta> ImageMetaSet = null;
    private String Category = null;

    public class CoverImage{
        private String SrcURL = null;
        private String StoreLocation = null;
        private String StoreID = null;
        private int Height= 0;
        private int Width = 0;
        private String MD5sum = null;

        @Override
        public String toString() {
            return "CoverImage{" +
                    "SrcURL='" + SrcURL + '\'' +
                    ", StoreLocation='" + StoreLocation + '\'' +
                    ", StoreID='" + StoreID + '\'' +
                    ", Height=" + Height +
                    ", Width=" + Width +
                    ", MD5sum='" + MD5sum + '\'' +
                    '}';
        }
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getSrcName() {
        return SrcName;
    }

    public void setSrcName(String srcName) {
        SrcName = srcName;
    }

    public String getSrcURL() {
        return SrcURL;
    }

    public void setSrcURL(String srcURL) {
        SrcURL = srcURL;
    }

    public String getTitle() {
        return Title;
    }

    public void setTitle(String title) {
        Title = title;
    }

    public String getDesc() {
        return Desc;
    }

    public void setDesc(String desc) {
        Desc = desc;
    }

    public HashSet<String> getTags() {
        return Tags;
    }

    public void setTags(HashSet<String> tags) {
        Tags = tags;
    }

    public void addTag(String tag){
        Tags.add(tag);
    }

    public HashSet<String> getAuthors() {
        return Authors;
    }

    public void setAuthors(HashSet<String> authors) {
        Authors = authors;
    }

    public void addAuthors(String author){
        Authors.add(author);
    }
    public CoverImage getCover() {
        return Cover;
    }

    public void setCover(CoverImage cover) {
        Cover = cover;
    }

    public Date getSeenTime() {
        return SeenTime;
    }

    public void setSeenTime(Date seenTime) {
        SeenTime = seenTime;
    }

    public Date getSrcCreateTime() {
        return SrcCreateTime;
    }

    public void setSrcCreateTime(Date srcCreateTime) {
        SrcCreateTime = srcCreateTime;
    }

    public Date getScrUpdateTime() {
        return ScrUpdateTime;
    }

    public void setScrUpdateTime(Date scrUpdateTime) {
        ScrUpdateTime = scrUpdateTime;
    }

    public HashSet<ImageMeta> getImageMetaSet() {
        return ImageMetaSet;
    }

    public void setImageMetaSet(HashSet<ImageMeta> imageMetaSet) {
        ImageMetaSet = imageMetaSet;
    }

    public void addImageMeata(ImageMeta im){
        ImageMetaSet.add(im);
    }

    public String getCategory() {
        return Category;
    }

    public void setCategory(String category) {
        Category = category;
    }

*/


    public void setIAMRecord(GenericRecord Record) {
        IAMRecord = Record;
    }

    public String getCategory() {
        try {
            return IAMRecord.get(Category).toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public String getSrcURL() {
        return IAMRecord.get(Src_URL).toString();
    }

    public String getID() {
        return IAMRecord.get(ID).toString();
    }

    public String getSeenTime() {
        return IAMRecord.get(SeenTime).toString();
    }

    public String getType(){
        return IAMRecord.get(Type).toString();
    }

    public String getSrcId(){
        return IAMRecord.get(SRC_ID).toString();
    }


    /*@Override
    public String toString() {
        return  IAMRecord.toString();
    }*/

    public String toFiltedString(){

        JSONObject jsonObj = new JSONObject(IAMRecord.toString());
        Iterator<String> it = jsonObj.keys();
        while (it.hasNext()){
            String key = it.next();
            Object ob = jsonObj.get(key);
            if(ob.equals(null)||jsonObj.isNull(key)){
                it.remove();
            }else if(ob instanceof String){
                if(((String) ob).trim().length()==0){
                    it.remove();
                }
            }
        }
        return jsonObj.toString();
    }
}
