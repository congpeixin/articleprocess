package cn.datapark.process.core.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * avro schema & data读取工具,支持从文件/inputstream读取schema,支持从二进制,json文本读取avro
 * Created by eason on 15/11/12.
 */
public class AvroUtil {

    private static final Logger LOG = Logger.getLogger(AvroUtil.class);


    private static Map<String, Schema> schemas = new HashMap<String, Schema>();

    private AvroUtil(){}

    public static void addSchema(String name, Schema schema){
        schemas.put(name, schema);
    }

    public static Schema getSchema(String name){
        return schemas.get(name);
    }

    public static Map<String, Schema> getSchemas() {
        return schemas;
    }

    public static String resolveSchema(String sc){
        String result = sc;

        for(Map.Entry<String, Schema> entry : schemas.entrySet()){
            result = replace(result, entry.getKey(), entry.getValue().toString());
        }

        return result;
    }

    static String replace(String str, String pattern, String replace) {
        int s = 0;
        int e = 0;
        StringBuilder result = new StringBuilder();

        while ((e = str.indexOf(pattern, s)) >= 0) {
            result.append(str.substring(s, e));
            result.append(replace);
            s = e+pattern.length();
        }

        result.append(str.substring(s));
        return result.toString();
    }

    public static Schema parseSchema(String fileName) throws Exception{
       return  parseSchema(new FileInputStream(fileName));
    }

    public static Schema parseSchema(InputStream in) throws Exception{
        String completeSchema = resolveSchema(read(in));
        Schema schema = new Schema.Parser().parse(completeSchema);
        String name = schema.getFullName();
        LOG.info("Schema Name is "+name);
        schemas.put(name, schema);
        return schema;
    }
    /*
     * Convert the file content into String
     */
    private static String read(String fileName) throws IOException {
        return read(new FileInputStream(fileName));
    }

    private static String read(InputStream in) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            return sb.toString();
        } finally {
            br.close();
        }
    }

    public static GenericDatumWriter<GenericRecord> getWriter(Schema name){
        return new GenericDatumWriter<GenericRecord>(name);
    }


    public static GenericRecord readFromAvroBinary(String writerSchemaNAme,String readerSchemaName,byte[] buffer){
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                    AvroUtil.getSchema(writerSchemaNAme),
                    AvroUtil.getSchema(readerSchemaName));
            Decoder decoder = DecoderFactory.get().binaryDecoder(buffer, null);
            return  reader.read(null, decoder);
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("failed to read from json binary");

    }

    public static GenericRecord readFromJsonString(String writerSchemaName,String readerSchemaName,String jsonString){
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                AvroUtil.getSchema(writerSchemaName),
                AvroUtil.getSchema(readerSchemaName));
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(AvroUtil.getSchema(readerSchemaName), jsonString);
            return  reader.read(null, decoder);
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("failed to read from json string");

    }

    public static GenericRecord readFromJsonString(String writerSchemaName,String readerSchemaName,InputStream inJson){

        BufferedReader br = new BufferedReader(new InputStreamReader(inJson));
        String jsonString;
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            jsonString = sb.toString();

            return readFromJsonString(writerSchemaName,readerSchemaName,jsonString);

        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            try {
                br.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new RuntimeException("failed to read from json inputstream");

    }

    public static GenericData.Record getGenericRecord(String name){
        return new GenericData.Record(schemas.get(name));
    }

    public static void main(String[] args){
        try{
            AvroUtil.parseSchema(AvroUtil.class.getClassLoader().getResourceAsStream("conf/coverimage_schema.avsc"));

            AvroUtil.parseSchema(AvroUtil.class.getClassLoader().getResourceAsStream("conf/image_schema.avsc"));

            Schema kafkaSchema = AvroUtil.parseSchema(AvroUtil.class.getClassLoader().getResourceAsStream("conf/album_schema_kafka.avsc"));

            LOG.info(kafkaSchema.toString(true));

            Schema esSchema = AvroUtil.parseSchema(AvroUtil.class.getClassLoader().getResourceAsStream("conf/album_schema_es.avsc"));

            LOG.info(esSchema.toString(true));

            //AvroUtil.read((AvroUtil.class.getClassLoader().getResourceAsStream("albumexample.json")));
            //GenericData.Record result =  AvroUtil.getGenericRecord("com.datapark.avro");
            //LOG.info(result.toString());

            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                    AvroUtil.getSchema("com.datapark.avro.kafkaalbummeta"),
                    AvroUtil.getSchema("com.datapark.avro.esalbummeta"));
            InputStream in = AvroUtil.class.getClassLoader().getResourceAsStream("conf/albumData.json");
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String jsonString;
            try {
                StringBuilder sb = new StringBuilder();
                String line = br.readLine();

                while (line != null) {
                    sb.append(line);
                    line = br.readLine();
                }
                jsonString = sb.toString();
            } finally {
                br.close();
            }
            Decoder decoder = DecoderFactory.get().jsonDecoder(AvroUtil.getSchema("com.datapark.avro.esalbummeta"), jsonString);
            GenericRecord result = reader.read(null, decoder);
            LOG.info(result.toString());

        }
        catch(Exception e){
            LOG.error("Exception thrown in combineSchema "+e);
        }
    }
}