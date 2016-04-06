package cn.datapark.process.core.config.callbacks;

import cn.datapark.process.core.config.services.ESConfigUpdateService;
import com.baidu.disconf.client.common.annotations.DisconfUpdateService;
import com.baidu.disconf.client.common.update.IDisconfUpdate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by eason on 15/9/9.
 */
@Service
@DisconfUpdateService(confFileKeys = {"es.properties"})
public class ESConfigUpdateCallback implements IDisconfUpdate {



    @Autowired
    private ESConfigUpdateService cfgUpdateService;

    public void reload() throws Exception {
        cfgUpdateService.changeConfig();
    }

}

