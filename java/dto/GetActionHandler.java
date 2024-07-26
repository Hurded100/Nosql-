package dto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.Store;
import utils.LoggerUtil;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class GetActionHandler implements ActionHandler {
    private static final Logger logger = LoggerFactory.getLogger(GetActionHandler.class);// 使用LoggerFactory获取Logger实例
    @Override
    public void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException {
        String value = store.get(dto.getKey());
        LoggerUtil.debug(logger, "[SocketServerHandler][run]: {}", "get action resp" + dto.getValue());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, value);
        oos.writeObject(resp);
        oos.flush();
    }
}
