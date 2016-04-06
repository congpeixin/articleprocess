package cn.datapark.process.core.config.services;

import cn.datapark.process.core.config.configs.ESConfig;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/9.
 */
@Service
@Scope("singleton")
public class ESConfigUpdateService implements InitializingBean, DisposableBean {


    private static final Logger LOG = Logger.getLogger(ESConfigUpdateService.class);

    /**
     * 分布式配置
     */
    @Autowired
    private ESConfig esConfig;

    /**
     * 关闭
     */
    public void destroy() throws Exception {

        LOG.info("destroy ==> ");

    }

    /**
     * 进行连接
     */
    public void afterPropertiesSet() throws Exception {

        LOG.info("connect ==> ");
    }

    /**
     * 后台更改值
     */
    public void changeConfig() {

        LOG.info("ESConfig changing to:");

        LOG.info("Cluster Name:" + esConfig.getClusterName() + " IP:" + esConfig.getClusterIP() + " port:" + esConfig.getClusterIP());

        LOG.info("change ok.");
    }
}