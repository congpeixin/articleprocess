package cn.datapark.process.article.avro.encoder;

import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by eason on 16/1/18.
 * 通用AvroEncoder接口
 */
public interface IAvroEncoder {

    /**
     * 读取并解析avro schema
     * @param schemaArray schema inputstream 列表,支持多个schema,如果有父子schema,则保证子schema在list中的位置在父schema之前
     * @param writerSchemaName 写record的schema 完整名称
     * @param readerSchemaName 读record的schema 完整名称
     */
    void parseSchema(ArrayList<InputStream> schemaArray, String writerSchemaName, String readerSchemaName);

    /**
     * 从Map中生成AvroRecord
     * @param map Map的key应与avro schema中的field名称相同(大小写敏感),avro的field为对应的值和类型;
     *            如果对应avro schenma的field为array,则map中相应的value为List<Object>,
     *            如果array中内容为自定义类型,则Value为List<Map<String,Object>,
     *            其中Map中的key对应自定义类型的field名称,以此类推进行嵌套.
     * @return 返回根据map和parseSchema中传入的writeSchema生成对应的GenericRecord
     */
    GenericRecord build(Map<String,Object> map);

    /**
     * 将avro record按照writerSchema 编码成为json字符串
     * @param record
     * @return
     */
    String encodeString(GenericRecord record);

    /**
     * 将avro record按照writerSchema 编码成为json二进制格式
     * @param record
     * @return
     **/
    String encodeBinary(GenericRecord record);

}
