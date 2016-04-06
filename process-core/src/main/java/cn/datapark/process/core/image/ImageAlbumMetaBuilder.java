package cn.datapark.process.core.image;

import cn.datapark.process.core.utils.AvroUtil;
import cn.datapark.process.core.utils.ESUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by eason on 15/10/31.
 */
public class ImageAlbumMetaBuilder {

    private static final Logger LOG = Logger.getLogger(ImageAlbumMetaBuilder.class);

    //private static Schema schemaKafka;
    //private static Schema schemaToES;

    //生成avro的schema
    public static String WriterSchemaName = "cn.datapark.avro.kafkaalbummeta";
    //读取面向es存储的schema
    public static String ReaderSchemaName= "cn.datapark.avro.esalbummeta";

    public class IAMBuilderException extends Exception{
        public IAMBuilderException(String message){
            super(message);
        }
    }

    /*public static void parseSchema(InputStream inKafkaSchema,InputStream inESSchema){
        Schema.Parser parser = new Schema.Parser();
        Schema.Parser parserToES = new Schema.Parser();

        try {
            schemaKafka = parser.parse(inKafkaSchema);
            schemaToES = parserToES.parse(inESSchema);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }*/


    /**
     * 初始化Schema,需要按照顺序初始化,从最子类的Schema开始初始化
     *
     * @param schemaArray 保存初始化的schenma inputstream,方法按照schemaArray中的索引从低到高顺序读入并初始化
     */
    public static void parseSchema(ArrayList<InputStream> schemaArray){

        try {
            for(int i=0;i<schemaArray.size();i++){
                Schema schema = AvroUtil.parseSchema(schemaArray.get(i));
                LOG.info(schema.toString(true));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /*public static ImageAlbumMeta buildFromAvroBinary(byte[] buffer) throws IAMBuilderException{

        ImageAlbumMeta iam = new ImageAlbumMeta();
        try {

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schemaKafka,schemaToES);
            Decoder decoder = DecoderFactory.get().binaryDecoder(buffer, null);
            GenericRecord result = reader.read(null, decoder);
            iam.setIAMRecord(result);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return  iam;
    }

    public static ImageAlbumMeta buildFromJsonString(String jsonString) throws IAMBuilderException{
        try {
            ImageAlbumMeta iam = new ImageAlbumMeta();
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schemaKafka,schemaToES);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schemaToES, jsonString);
            GenericRecord result = reader.read(null, decoder);

            result.put("id", ESUtil.buildESDocIDFromURL(result.get("url").toString()));
            iam.setIAMRecord(result);
            return iam;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }*/

    /**
     * 从二进制中读取avro record
     * @param buffer
     * @return
     * @throws IAMBuilderException
     */
    public static ImageAlbumMeta buildFromAvroBinary(byte[] buffer) throws IAMBuilderException{

        ImageAlbumMeta iam = new ImageAlbumMeta();
        try {

            GenericRecord result = AvroUtil.readFromAvroBinary(WriterSchemaName,ReaderSchemaName,buffer);

            iam.setIAMRecord(result);

            return  iam;

        } catch (Exception e) {
            e.printStackTrace();
        }

        throw new RuntimeException("failed to read from avro binary");
    }

    /**
     * 从json文本中读取avro record
     * @param jsonString
     * @return
     * @throws IAMBuilderException
     */
    public static ImageAlbumMeta buildFromJsonString(String jsonString) throws IAMBuilderException{

        ImageAlbumMeta iam = new ImageAlbumMeta();
        GenericRecord result = AvroUtil.readFromJsonString(WriterSchemaName,ReaderSchemaName,jsonString);

        result.put(ImageAlbumMeta.ID, ESUtil.buildESDocIDFromURL(result.get(ImageAlbumMeta.Src_URL).toString()));

        iam.setIAMRecord(result);
        return iam;
    }


    /**
     * 从json文本中读取avro record
     * @param inJson
     * @return
     * @throws IAMBuilderException
     */
    public static ImageAlbumMeta buildFromJsonString(InputStream inJson) throws IAMBuilderException{

        ImageAlbumMeta iam = new ImageAlbumMeta();
        GenericRecord result = AvroUtil.readFromJsonString(WriterSchemaName,ReaderSchemaName,inJson);

        result.put(ImageAlbumMeta.ID, ESUtil.buildESDocIDFromURL(result.get(ImageAlbumMeta.Src_URL).toString()));
        iam.setIAMRecord(result);
        return iam;

    }

}
