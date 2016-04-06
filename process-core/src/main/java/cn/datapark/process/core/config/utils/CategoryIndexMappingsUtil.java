package cn.datapark.process.core.config.utils;

import cn.datapark.process.core.config.configs.CategoryIndexMappings;
import cn.datapark.process.core.config.configs.ESConfig;
import org.json.JSONArray;

/**
 * Created by eason on 15/10/13.
 */
public class CategoryIndexMappingsUtil {
    /**
     * 根据JSONArray形式的category返回对应恶ES IndexType
     * @param jsonArrayCategory
     * @return
     */
    public static String getIndexType(CategoryIndexMappings mappings,JSONArray jsonArrayCategory){


        ESConfig esConfig = DistributedConfigUtil.getInstance().getDistributedConfigCollection().getESConfig();

        String indexType = null;

        if (jsonArrayCategory != null) {
            //根据json category字段数组第一组字符串，判断大类别

        }
        return  indexType;

    }
}
