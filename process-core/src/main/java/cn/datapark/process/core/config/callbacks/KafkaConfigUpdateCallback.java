package cn.datapark.process.core.config.callbacks;

import cn.datapark.process.core.config.services.KafkaConfigUpdateService;
import com.baidu.disconf.client.common.annotations.DisconfUpdateService;
import com.baidu.disconf.client.common.update.IDisconfUpdate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/12.
 */
@Service
@DisconfUpdateService(confFileKeys = {"kafka.properties"})
public class KafkaConfigUpdateCallback implements IDisconfUpdate {

    @Autowired
    private KafkaConfigUpdateService cfgUpdateService;

    public void reload() throws Exception {
        cfgUpdateService.changeConfig();
    }

}
