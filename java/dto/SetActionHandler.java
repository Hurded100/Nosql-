package dto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory; // 导入LoggerFactory
import service.Store;
import utils.LoggerUtil;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class SetActionHandler implements ActionHandler {
    private static final Logger logger = LoggerFactory.getLogger(SetActionHandler.class); // 使用LoggerFactory获取Logger实例
    @Override
    public void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException {
        store.set(dto.getKey(), dto.getValue());
        LoggerUtil.debug(logger, "[SocketServerHandler][run]: {}", "set action resp" + dto.getValue());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, dto.getValue());
        oos.writeObject(resp);
        oos.flush();
    }
}

