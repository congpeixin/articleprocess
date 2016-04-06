package cn.datapark.process.article.avro.decoder;

import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by eason on 16/1/12.
 */
public interface IAvroDecoder {

    /**
     * 读取并解析avro schema
     * @param schemaArray schema inputstream 列表,支持多个schema,如果有父子schema,则保证子schema在list中的位置在父schema之前
     * @param writerSchemaName 写record的schema 完整名称
     * @param readerSchemaName 读record的schema 完整名称
     */
    void parseSchema(ArrayList<InputStream> schemaArray, String writerSchemaName, String readerSchemaName);
    /**
     * 从二进制中读取avro record
     * @param buffer
     * @return
     * @throws AvroDecoderException
     */
    public GenericRecord buildFromAvroBinary(byte[] buffer) throws AvroDecoderException;

    /**
     * 从json文本中读取avro record
     * @param jsonString
     * @return
     * @throws AvroDecoderException
     */
    public  GenericRecord buildFromJsonString(String jsonString) throws AvroDecoderException;


    /**
     * 从json inputstream中读取avro record
     * @param inJson
     * @return
     * @throws AvroDecoderException
     */
    public  GenericRecord buildFromJsonString(InputStream inJson) throws AvroDecoderException;
}
