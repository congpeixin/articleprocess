package cn.datapark.process.core.config.utils;

import cn.datapark.process.core.config.configs.DPKafkaConfig;
import cn.datapark.process.core.config.configs.ESConfig;
import cn.datapark.process.core.config.configs.CategoryIndexMappings;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/11.
 * esconfig，kafkaconfig，esmapping的配置管理集合
 */
@Service
public class DistributedConfigCollection {

    private static final Logger LOG = Logger.getLogger(DistributedConfigCollection.class);


    @Autowired
    private ESConfig esConfig;

    @Autowired
    private DPKafkaConfig dpKafkaConfig;

    private CategoryIndexMappings categoryIndexMappings = new CategoryIndexMappings();

    public DistributedConfigCollection(){

    }

    public ESConfig getESConfig(){
        return esConfig;
    }

    public DPKafkaConfig getDPKafkaConfig(){
        return dpKafkaConfig;
    }

    public CategoryIndexMappings getCategoryIndexMappings(){
        return  categoryIndexMappings;
    }


    public  void init(String resourcePath) throws ConfigurationException {
        categoryIndexMappings.init(resourcePath);
    }
}
