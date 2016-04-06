package cn.datapark.process.core.config.configs;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/8.
 * 用baidu disconf开源组件进行配置管理
 * 需要用到spring环境
 */
@Service
@Scope("singleton")
@DisconfFile(filename = "es.properties")
public class ESConfig {

    private String ClusterName ;
    private String ClusterIP;
    private int ClusterPort;
    private String MappingFile;



    @DisconfFileItem(name = "cluster.name",associateField = "ClusterName")
    public String getClusterName() {
        return ClusterName;
    }

    public void setClusterName(String clusterName) {
        ClusterName = clusterName;
    }

    @DisconfFileItem(name = "cluster.ip",associateField = "ClusterIP")
    public String getClusterIP() {
        return ClusterIP;
    }

    public void setClusterIP(String clusterIP) {
        ClusterIP = clusterIP;
    }

    @DisconfFileItem(name = "cluster.port",associateField = "ClusterPort")
    public int getClusterPort() {
        return ClusterPort;
    }

    public  void setClusterPort(int clusterPort){
        ClusterPort = clusterPort;
    }

    @DisconfFileItem(name = "catindexmapping.file",associateField = "MappingFile")
    public String getMappingFile() {
        return MappingFile;
    }

    public void setMappingFile(String mappingFile) {
        MappingFile = mappingFile;
    }



}

