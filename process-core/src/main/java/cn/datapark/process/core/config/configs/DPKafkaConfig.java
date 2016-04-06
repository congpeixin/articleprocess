package cn.datapark.process.core.config.configs;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/12.
 * kafka配置
 * 利用spring和百度disconf自动注入
 */
@Service
@Scope("singleton")
@DisconfFile(filename = "kafka.properties")
public class DPKafkaConfig {
    private String Topic;
    private String ReSKUTopic;// 重新出来的topic
    private String ReSKUConsumer;//重新处理 consumer
    private String ConsumerRoot;
    private String ZookeeperServer;
    private long maxOffsetBehind = Long.MAX_VALUE;
    private boolean useStartOffsetTimeIfOffsetOutOfRange = true;
    private boolean forceFromStart = false;
    private long startOffsetTime = kafka.api.OffsetRequest.LatestTime();

    @DisconfFileItem(name = "kafka.topic",associateField = "Topic")
    public String getTopic() {
        return Topic;
    }

    public void setTopic(String topic) {
        Topic = topic;
    }

    @DisconfFileItem(name = "kafka.consumer.root",associateField = "ConsumerRoot")
    public String getConsumerRoot() {
        return ConsumerRoot;
    }

    public void setConsumerRoot(String consumerRoot) {
        ConsumerRoot = consumerRoot;
    }

    @DisconfFileItem(name = "kafka.zookeeper.server",associateField = "ZookeeperServer")
    public String getZookeeperServer() {
        return ZookeeperServer;
    }

    public void setZookeeperServer(String zookeeperServer) {
        ZookeeperServer = zookeeperServer;
    }


    @DisconfFileItem(name = "kafka.spout.maxoffsetbehind",associateField = "maxOffsetBehind")
    public long getMaxOffsetBehind() {
        return maxOffsetBehind;
    }

    public void setMaxOffsetBehind(long maxOffsetBehind) {
        this.maxOffsetBehind = maxOffsetBehind;
    }

    @DisconfFileItem(name = "kafka.spout.usestartOffsettimeifoffsetoutofrange",associateField = "useStartOffsetTimeIfOffsetOutOfRange")
    public boolean isUseStartOffsetTimeIfOffsetOutOfRange() {
        return useStartOffsetTimeIfOffsetOutOfRange;
    }

    public void setUseStartOffsetTimeIfOffsetOutOfRange(boolean useStartOffsetTimeIfOffsetOutOfRange) {
        this.useStartOffsetTimeIfOffsetOutOfRange = useStartOffsetTimeIfOffsetOutOfRange;
    }

    @DisconfFileItem(name = "kafka.spout.forcefromstart",associateField = "forceFromStart")
    public boolean isForceFromStart() {
        return forceFromStart;
    }

    public void setForceFromStart(boolean forceFromStart) {
        this.forceFromStart = forceFromStart;
    }

    @DisconfFileItem(name = "kafka.spout.startoffsettime",associateField = "startOffsetTime")
    public long getStartOffsetTime() {
        return startOffsetTime;
    }

    public void setStartOffsetTime(long startOffsetTime) {
        this.startOffsetTime = startOffsetTime;
    }

    @DisconfFileItem(name = "kafka.resku.topic",associateField = "ReSKUTopic")
    public String getReSKUTopic() {
        return ReSKUTopic;
    }

    public void setReSKUTopic(String reSKUTopic) {
        ReSKUTopic = reSKUTopic;
    }
    @DisconfFileItem(name = "kafka.resku.consumer",associateField = "ReSKUConsumer")
    public String getReSKUConsumer() {
        return ReSKUConsumer;
    }

    public void setReSKUConsumer(String reSKUConsumer) {
        ReSKUConsumer = reSKUConsumer;
    }
}
