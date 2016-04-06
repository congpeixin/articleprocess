package cn.datapark.process.article.avro.encoder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eason on 16/1/18.
 */
public class AvroEncoder implements IAvroEncoder {

    private static final Logger LOG = Logger.getLogger(AvroEncoder.class);

    //生成avro的schema
    protected String WriterSchemaName = null;
    //存储的schema
    protected String ReaderSchemaName = null;

    private Map<String, Schema> schemas = new HashMap<String, Schema>();


    private String resolveSchema(String sc) {
        String result = sc;

        for (Map.Entry<String, Schema> entry : schemas.entrySet()) {
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
            s = e + pattern.length();
        }

        result.append(str.substring(s));
        return result.toString();
    }

    private Schema parseSchema(String fileName) throws Exception {
        return parseSchema(new FileInputStream(fileName));
    }

    private Schema parseSchema(InputStream in) throws Exception {
        String completeSchema = resolveSchema(read(in));
        Schema schema = new Schema.Parser().parse(completeSchema);
        String name = schema.getFullName();
        LOG.info("Schema Name is " + name);
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

    protected Schema getSchema(String name) {
        return schemas.get(name);
    }

    private GenericDatumWriter<GenericRecord> getWriter(Schema name) {
        return new GenericDatumWriter<GenericRecord>(name);
    }

    public GenericData.Record getGenericRecord(String name) {
        return new GenericData.Record(schemas.get(name));
    }

    public void parseSchema(ArrayList<InputStream> schemaList, String writerSchemaName, String readerSchemaName) {

        if ((readerSchemaName == null) || (writerSchemaName == null)) {
            LOG.error("readerSchemaName and writerSchemaName not allow null");
        }
        WriterSchemaName = writerSchemaName;
        ReaderSchemaName = readerSchemaName;

        try {
            for (int i = 0; i < schemaList.size(); i++) {
                Schema schema = parseSchema(schemaList.get(i));
                LOG.info(schema.toString(true));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public GenericRecord build(Map<String, Object> map) {
        return buildRecord(map, getSchema(WriterSchemaName));
    }

    /**
     * 根据schema生成avro record,通过嵌套调用自身,实现avro schema嵌套自定义schema数据结构的处理
     *
     * @param map
     * @param schema
     * @return
     */
    private GenericRecord buildRecord(Map<String, Object> map, Schema schema) {

        GenericRecord record = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();
        //遍历schema中的field
        for (Schema.Field field : fields) {
            String fieldName = field.name();

            if (field.schema().getType().equals(Schema.Type.RECORD)) {
                if (map.get(fieldName) instanceof Map) {
                    GenericRecord nestedRecord = buildRecord((Map<String, Object>) map.get(fieldName), field.schema());
                    record.put(fieldName, nestedRecord);
                } else {
                    //record.put(fieldName,null);
                    LOG.error("only Map<String,Object> type support for nested object");
                }

            } else if (field.schema().getType().equals(Schema.Type.ARRAY)) {
                if (map.get(fieldName) != null) {
                    if (map.get(fieldName) instanceof List) {
                        Schema elemetSchema = field.schema().getElementType();

                        List<Map<String, Object>> arrayElement = (List<Map<String, Object>>) map.get(fieldName);

                        List<GenericRecord> subRecords = new ArrayList<GenericRecord>();
                        for (Map<String, Object> elementMap : arrayElement) {
                            subRecords.add(buildRecord(elementMap, elemetSchema));
                        }
                        record.put(fieldName, subRecords);

                    } else if (map.get(fieldName) instanceof String) {

                        List<String> listElemet = (List<String>) map.get(fieldName);

                        record.put(fieldName, listElemet);
                    } else {
                        //record.put(fieldName,null);
                        LOG.error("only Map<String,Object> ,List<String> type support for array ");
                    }
                } else {

                    if (field.schema().getElementType().getType().equals(Schema.Type.STRING)) {
                        ArrayList<String> list = new ArrayList<String>();
                        for (int i = 0; i < field.defaultValue().size(); i++) {
                            list.add(field.defaultValue().get(i).asText());
                        }
                        record.put(fieldName, list);
                    } else if (field.schema().getElementType().getType().equals(Schema.Type.BOOLEAN)) {
                        ArrayList<Boolean> list = new ArrayList<Boolean>();
                        for (int i = 0; i < field.defaultValue().size(); i++) {
                            list.add(field.defaultValue().get(i).asBoolean());
                        }
                        record.put(fieldName, list);
                    } else if (field.schema().getElementType().getType().equals(Schema.Type.DOUBLE)) {
                        ArrayList<Double> list = new ArrayList<Double>();
                        for (int i = 0; i < field.defaultValue().size(); i++) {
                            list.add(field.defaultValue().get(i).asDouble());
                        }
                        record.put(fieldName, list);
                    } else if (field.schema().getElementType().getType().equals(Schema.Type.FLOAT)) {
                        ArrayList<Float> list = new ArrayList<Float>();
                        for (int i = 0; i < field.defaultValue().size(); i++) {
                            list.add((float) field.defaultValue().get(i).asDouble());
                        }
                        record.put(fieldName, list);
                    } else if (field.schema().getElementType().getType().equals(Schema.Type.INT)) {
                        ArrayList<Integer> list = new ArrayList<Integer>();
                        for (int i = 0; i < field.defaultValue().size(); i++) {
                            list.add(field.defaultValue().get(i).asInt());
                        }
                        record.put(fieldName, list);
                    } else if (field.schema().getElementType().getType().equals(Schema.Type.LONG)) {
                        ArrayList<Long> list = new ArrayList<Long>();
                        for (int i = 0; i < field.defaultValue().size(); i++) {
                            list.add(field.defaultValue().get(i).asLong());
                        }
                        record.put(fieldName, list);
                    }
                }
            } else {
                Object value = map.get(fieldName);
                if (value != null) {
                    record.put(fieldName, value);

                } else {
                    //设置默认值
                    if (field.schema().getType().equals(Schema.Type.STRING)) {
                        if (field.defaultValue() != null)
                            record.put(fieldName, field.defaultValue().asText());
                    } else if (field.schema().getType().equals(Schema.Type.BOOLEAN)) {
                        if (field.defaultValue() != null)
                            record.put(fieldName, field.defaultValue().asBoolean());
                    } else if (field.schema().getType().equals(Schema.Type.DOUBLE)) {
                        if (field.defaultValue() != null)
                            record.put(fieldName, field.defaultValue().asDouble());
                    } else if (field.schema().getType().equals(Schema.Type.FLOAT)) {
                        if (field.defaultValue() != null)
                            record.put(fieldName, field.defaultValue().asDouble());
                    } else if (field.schema().getType().equals(Schema.Type.INT)) {
                        if (field.defaultValue() != null)
                            record.put(fieldName, field.defaultValue().asInt());
                    } else if (field.schema().getType().equals(Schema.Type.LONG)) {
                        if (field.defaultValue() != null)
                            record.put(fieldName, field.defaultValue().asLong());
                    } else if (field.schema().getType().equals(Schema.Type.NULL)) {
                        record.put(fieldName, null);
                    }
                }


            }
        }
        return record;
    }

    public String encodeString(GenericRecord record) {

        try {

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(getSchema(WriterSchemaName));
            JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(getSchema(WriterSchemaName), out);
            writer.write(record, jsonEncoder);
            jsonEncoder.flush();
            out.flush();
            return out.toString();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Todo 该功能未完成
     *
     * @param record
     * @return
     */
    public String encodeBinary(GenericRecord record) {
        return null;
    }
}
