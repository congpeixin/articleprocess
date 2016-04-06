package cn.datapark.process.article.bolts;

import cn.datapark.process.article.model.ImageDownloadRequest;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by eason on 16/2/29.
 */
public class ImageDownloadRequestSerializer extends Serializer<ImageDownloadRequest> {
    @Override
    public void write(Kryo kryo, Output output, ImageDownloadRequest object) {
        try {
            output.writeString(object.srcName);
            output.writeString(object.srcUrl);
            output.writeString(object.seenTime);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(object.urlMap);
            String serStr = byteArrayOutputStream.toString("ISO-8859-1");
            serStr = java.net.URLEncoder.encode(serStr, "UTF-8");
            output.writeString(serStr);
            objectOutputStream.close();
            byteArrayOutputStream.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public ImageDownloadRequest read(Kryo kryo, Input input, Class<ImageDownloadRequest> type) {
        ImageDownloadRequest idr = new ImageDownloadRequest();

        try {
            idr.srcName = input.readString();
            idr.srcUrl = input.readString();
            idr.seenTime = input.readString();
            String redStr = java.net.URLDecoder.decode(input.readString(), "UTF-8");
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(redStr.getBytes("ISO-8859-1"));
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            HashMap<String,Set<String>> newHash = (HashMap<String,Set<String>>)objectInputStream.readObject();
            objectInputStream.close();
            byteArrayInputStream.close();
            idr.urlMap = newHash;

        }catch (Exception e){
            e.printStackTrace();
        }


        return idr;
    }
}
