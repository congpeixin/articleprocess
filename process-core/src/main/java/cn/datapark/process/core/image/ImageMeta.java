package cn.datapark.process.core.image;

import java.util.HashSet;

/**
 * Created by eason on 15/10/9.
 */
public class ImageMeta {

    private String ID = null;
    private String SrcURL = null;
    private String StoreLocation = null;
    private String StoreID = null;
    private String Title = null;
    private HashSet<String> Tags = new HashSet<String>();
    private HashSet<String> Authors = new HashSet<String>();
    private String Desc = null;
    private int Height= 0;
    private int Width = 0;
    private String MD5sum = null;

    public int getWidth() {
        return Width;
    }

    public void setWidth(int width) {
        Width = width;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getSrcURL() {
        return SrcURL;
    }

    public void setSrcURL(String srcURL) {
        SrcURL = srcURL;
    }

    public String getStoreLocation() {
        return StoreLocation;
    }

    public void setStoreLocation(String storeLocation) {
        StoreLocation = storeLocation;
    }

    public String getStoreID() {
        return StoreID;
    }

    public void setStoreID(String storeID) {
        StoreID = storeID;
    }

    public String getTitle() {
        return Title;
    }

    public void setTitle(String title) {
        Title = title;
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

    public void addAuthor(String author){
        Authors.add(author);
    }
    public void setAuthors(HashSet<String> authors) {
        Authors = authors;
    }

    public String getDesc() {
        return Desc;
    }

    public void setDesc(String desc) {
        Desc = desc;
    }

    public int getHeight() {
        return Height;
    }

    public void setHeight(int height) {
        Height = height;
    }

    public String getMD5sum() {
        return MD5sum;
    }

    public void setMD5sum(String MD5) {
        this.MD5sum = MD5sum;
    }

    @Override
    public String toString() {
        return "ImageMeta{" +
                "ID='" + ID + '\'' +
                ", SrcURL='" + SrcURL + '\'' +
                ", StoreLocation='" + StoreLocation + '\'' +
                ", StoreID='" + StoreID + '\'' +
                ", Title='" + Title + '\'' +
                ", Tags=" + Tags +
                ", Authors=" + Authors +
                ", Desc='" + Desc + '\'' +
                ", Height=" + Height +
                ", Width=" + Width +
                ", MD5sum='" + MD5sum + '\'' +
                '}';
    }
}
