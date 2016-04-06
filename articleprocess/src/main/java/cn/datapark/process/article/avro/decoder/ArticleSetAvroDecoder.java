package cn.datapark.process.article.avro.decoder;

import org.apache.avro.generic.GenericRecord;

import java.io.InputStream;

/**
 * Created by eason on 16/1/12.
 */
public class ArticleSetAvroDecoder extends AvroDecoder {



    /**
     * 从二进制中读取avro record
     * @param buffer
     * @return
     * @throws AvroDecoderException
     */
    public ArticleSetAvroRecord buildArticleSetFromAvroBinary(byte[] buffer) throws AvroDecoderException {


            GenericRecord result = buildFromAvroBinary(buffer);
            ArticleSetAvroRecord asr = new ArticleSetAvroRecord();
            asr.setArticleSet(result);
            return  asr;

    }

    /**
     * 从json文本中读取avro record
     * @param jsonString
     * @return
     * @throws AvroDecoderException
     */
    public ArticleSetAvroRecord buildArticleSetFromJsonString(String jsonString) throws AvroDecoderException {

            GenericRecord result = buildFromJsonString(jsonString);
            ArticleSetAvroRecord asr = new ArticleSetAvroRecord();
            asr.setArticleSet(result);

            return  asr;

    }


    /**
     * 从json文本中读取avro record
     * @param inJson
     * @return
     * @throws AvroDecoderException
     */
    public  ArticleSetAvroRecord buildArticleSetFromJsonString(InputStream inJson) throws AvroDecoderException {

        GenericRecord result = buildFromJsonString(inJson);
        ArticleSetAvroRecord asr = new ArticleSetAvroRecord();
        asr.setArticleSet(result);

        return  asr;

    }

}
