package cn.datapark.process.article.config;

/**
 * Created by eason on 16/1/15.
 */
public class KafkaConfig {
    public String ZookeeperServer;
    public String Topic;
    public String ZookeeperRoot;
    public String ConsumerMaxOffsetBehind;
    public String ConsumerUseStartOffsetTimeIfOffSetOutOfRange;
    public String ConsumerForceFromStart;
    public String ConsumerStartOffsetTime;


    public String getZookeeperServer() {
        return ZookeeperServer;
    }

    public void setZookeeperServer(String zookeeperServer) {
        ZookeeperServer = zookeeperServer;
    }

    public String getTopic() {
        return Topic;
    }

    public void setTopic(String topic) {
        Topic = topic;
    }

    public String getZookeeperRoot() {
        return ZookeeperRoot;
    }

    public void setZookeeperRoot(String zookeeperRoot) {
        ZookeeperRoot = zookeeperRoot;
    }

    public String getConsumerMaxOffsetBehind() {
        return ConsumerMaxOffsetBehind;
    }

    public void setConsumerMaxOffsetBehind(String consumerMaxOffsetBehind) {
        ConsumerMaxOffsetBehind = consumerMaxOffsetBehind;
    }

    public String getConsumerUseStartOffsetTimeIfOffSetOutOfRange() {
        return ConsumerUseStartOffsetTimeIfOffSetOutOfRange;
    }

    public void setConsumerUseStartOffsetTimeIfOffSetOutOfRange(String consumerUseStartOffsetTimeIfOffSetOutOfRange) {
        ConsumerUseStartOffsetTimeIfOffSetOutOfRange = consumerUseStartOffsetTimeIfOffSetOutOfRange;
    }

    public String getConsumerForceFromStart() {
        return ConsumerForceFromStart;
    }

    public void setConsumerForceFromStart(String consumerForceFromStart) {
        ConsumerForceFromStart = consumerForceFromStart;
    }

    public String getConsumerStartOffsetTime() {
        return ConsumerStartOffsetTime;
    }

    public void setConsumerStartOffsetTime(String consumerStartOffsetTime) {
        ConsumerStartOffsetTime = consumerStartOffsetTime;
    }
}