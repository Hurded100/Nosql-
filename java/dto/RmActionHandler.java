package dto;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import service.Store;
import utils.LoggerUtil;

import java.io.IOException;
import java.io.ObjectOutputStream;

public class RmActionHandler implements ActionHandler {
    private static final Logger logger = LoggerFactory.getLogger(RmActionHandler.class);
    @Override
    public void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException {
        store.rm(dto.getKey());
        LoggerUtil.debug(logger, "[SocketServerHandler][run]: {}", "remove action resp" + dto.toString());
        RespDTO resp = new RespDTO(RespStatusTypeEnum.SUCCESS, null);
        oos.writeObject(resp);
        oos.flush();
    }
}

