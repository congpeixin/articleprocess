package cn.datapark.process.article.avro.decoder;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by eason on 15/10/31.
 */

public class AvroDecoder implements IAvroDecoder {

    private static final Logger LOG = Logger.getLogger(AvroDecoder.class);

    //生成avro的schema
    protected   String WriterSchemaName = null;
    //存储的schema
    protected String ReaderSchemaName = null;

    private  Map<String, Schema> schemas = new HashMap<String, Schema>();


    private  String resolveSchema(String sc){
        String result = sc;

        for(Map.Entry<String, Schema> entry : schemas.entrySet()){
            result = replace(result, entry.getKey(), entry.getValue().toString());
        }

        return result;
    }

    private String replace(String str, String pattern, String replace) {
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

    private Schema parseSchema(String fileName) throws Exception{
        return  parseSchema(new FileInputStream(fileName));
    }

    private  Schema parseSchema(InputStream in) throws Exception{
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
    private String read(String fileName) throws IOException {
        return read(new FileInputStream(fileName));
    }

    private String read(InputStream in) throws IOException {
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

    private GenericDatumWriter<GenericRecord> getWriter(Schema name){
        return new GenericDatumWriter<GenericRecord>(name);
    }


    private GenericRecord readFromAvroBinary(String writerSchemaNAme,String readerSchemaName,byte[] buffer){
        try {
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                    getSchema(writerSchemaNAme),
                    getSchema(readerSchemaName));
            Decoder decoder = DecoderFactory.get().binaryDecoder(buffer, null);
            return  reader.read(null, decoder);
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("failed to read from json binary");

    }

    private GenericRecord readFromJsonString(String writerSchemaName,String readerSchemaName,String jsonString){
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
                getSchema(writerSchemaName),
                getSchema(readerSchemaName));
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(getSchema(readerSchemaName), jsonString);
            return  reader.read(null, decoder);
        }catch (Exception e){
            e.printStackTrace();
        }
        throw new RuntimeException("failed to read from json string");

    }

    private GenericRecord readFromJsonString(String writerSchemaName,String readerSchemaName,InputStream inJson){

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
    private Schema getSchema(String name){
        return schemas.get(name);
    }

    public  GenericData.Record getGenericRecord(String name){
        return new GenericData.Record(schemas.get(name));
    }
    /**
     * 初始化Schema,需要按照顺序初始化,从最子类的Schema开始初始化
     *
     * @param schemaList 保存初始化的schenma inputstream,方法按照schemaArray中的索引从低到高顺序读入并初始化
     */
    public  void parseSchema(ArrayList<InputStream> schemaList,String writerSchemaName,String readerSchemaName){

        if((readerSchemaName == null)||(writerSchemaName == null)){
            LOG.error("readerSchemaName and writerSchemaName not allow null");
        }
        WriterSchemaName = writerSchemaName;
        ReaderSchemaName = readerSchemaName;

        try {
            for(int i=0;i<schemaList.size();i++){
                Schema schema = parseSchema(schemaList.get(i));
                LOG.info(schema.toString(true));
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
    /**
     * 从二进制中读取avro record
     * @param buffer
     * @return
     * @throws AvroDecoderException
     */
    public  GenericRecord buildFromAvroBinary(byte[] buffer) throws AvroDecoderException {


        try {

            GenericRecord result = readFromAvroBinary(WriterSchemaName,ReaderSchemaName,buffer);


            return  result;

        } catch (Exception e) {
            e.printStackTrace();
        }

        throw new AvroDecoderException("failed to read from avro binary");
    }

    /**
     * 从json文本中读取avro record
     * @param jsonString
     * @return
     * @throws AvroDecoderException
     */
    public  GenericRecord buildFromJsonString(String jsonString) throws AvroDecoderException {

        GenericRecord result = readFromJsonString(WriterSchemaName,ReaderSchemaName,jsonString);

        return result;
    }


    /**
     * 从json inputstream中读取avro record
     * @param inJson
     * @return
     * @throws AvroDecoderException
     */
    public  GenericRecord buildFromJsonString(InputStream inJson) throws AvroDecoderException {

        GenericRecord result = readFromJsonString(WriterSchemaName,ReaderSchemaName,inJson);

        return result;

    }

}
