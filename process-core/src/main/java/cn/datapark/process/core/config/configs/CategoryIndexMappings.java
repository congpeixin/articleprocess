package cn.datapark.process.core.config.configs;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.json.JSONArray;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**
 * Created by eason on 15/10/13.
 */
public class CategoryIndexMappings {

    private XMLConfiguration mappingsConfig = null;

    private HashMap<String,String> indexSchemas = new HashMap<String,String>();

    private HashSet<CIMapping> CIMappings = new HashSet<CIMapping>();

    private class CIMapping{
        public String category;
        public String indexName;
        public String indexType;
    }

    /**
     * 从xml配置文件中初始化mapping内容
     * @param resourcePath
     * @throws ConfigurationException
     */
    public void init(String resourcePath) throws ConfigurationException {

        mappingsConfig = new XMLConfiguration(CategoryIndexMappings.class.getClassLoader().getResource((resourcePath)));

        //遍历读取所有index schema
        List<HierarchicalConfiguration> schemas = mappingsConfig.configurationsAt("schemas.index");
        for(HierarchicalConfiguration index : schemas) {
            indexSchemas.put(index.getString("name"), index.getString("schema"));
        }

        //遍历读取所有category 和 indextype的映射
        List<HierarchicalConfiguration> maps = mappingsConfig.configurationsAt("mappings.mapping");
        for(HierarchicalConfiguration map : maps) {
            CIMapping cim = new CIMapping();
            cim.category = map.getString("category");
            cim.indexName = map.getString("indexname");
            cim.indexType = map.getString("indextype");
            CIMappings.add(cim);
        }
    }

    /**
     * 根据JSONArray形式的category返回对应恶ES IndexType
     * @param jsonArrayCategory
     * @return
     */
    public String getIndexType(JSONArray jsonArrayCategory){


        String indexType = null;

        if (jsonArrayCategory != null) {
            //根据json category字段数组第一组字符串，判断大类别
            Iterator it= CIMappings.iterator();
            while(it.hasNext()){
                CIMapping cim = (CIMapping)it.next();
                if(cim.category.equalsIgnoreCase(jsonArrayCategory.getString(0))){
                    return cim.indexType;
                }
            }
        }
        return  indexType;

    }


    /**
     * 根据String形式的category返回对应恶ES IndexType
     * @param cat
     * @return
     */
    public String getIndexType(String cat){


        String indexType = null;

        if (cat != null) {
            //根据json category字段数组第一组字符串，判断大类别
            Iterator it= CIMappings.iterator();
            while(it.hasNext()){
                CIMapping cim = (CIMapping)it.next();
                if(cim.category.equalsIgnoreCase(cat)){
                    return cim.indexType;
                }
            }
        }
        return  indexType;

    }


    /**
     * 根据JSONArray形式的category返回对应恶ES IndexName
     * @param jsonArrayCategory
     * @return
     */
    public String getIndexName(JSONArray jsonArrayCategory){


        String indexType = null;

        if (jsonArrayCategory != null) {
            //根据json category字段数组第一组字符串，判断大类别
            Iterator it= CIMappings.iterator();
            while(it.hasNext()){
                CIMapping cim = (CIMapping)it.next();
                if(cim.category.equalsIgnoreCase(jsonArrayCategory.getString(0))){
                    return cim.indexName;
                }
            }
        }
        return  indexType;

    }

    /**
     * 根据String形式的category返回对应恶ES IndexName
     * @param cat
     * @return
     */
    public String getIndexName(String cat){


        String indexType = null;

        if (cat != null) {
            //根据json category字段数组第一组字符串，判断大类别
            Iterator it= CIMappings.iterator();
            while(it.hasNext()){
                CIMapping cim = (CIMapping)it.next();
                if(cim.category.equalsIgnoreCase(cat)){
                    return cim.indexName;
                }
            }
        }
        return  indexType;

    }

    /**
     * 获取所有的es mapping schema
     * @return hashmap key为indexname，value为shcema json文件
     */
    public HashMap<String,String> getSchemas(){
        return indexSchemas;
    }

    public static void main(String[] args){
        XMLConfiguration mappingsConfig = null;
        InputStream in  = CategoryIndexMappings.class.getClassLoader().getResourceAsStream(("catindexmapping.xml"));

        try {
            mappingsConfig = new XMLConfiguration(CategoryIndexMappings.class.getClassLoader().getResource(("catindexmapping.xml")));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }


        byte[] buffer = new byte[1024];
        try {
            in.read(buffer);
            System.out.println(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //遍历读取所有index schema
        List<HierarchicalConfiguration> schemas = mappingsConfig.configurationsAt("schemas");
        for(HierarchicalConfiguration index : schemas) {
            index.toString();
        }

        //遍历读取所有category 和 indextype的映射
        List<HierarchicalConfiguration> maps = mappingsConfig.configurationsAt("mappings");

    }
}
