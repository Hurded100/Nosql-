/*
 *@Type SocketServerHandler.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 12:50
 * @version
 */
package controller;

import dto.*;
import service.NormalStore;
import service.Store;
import utils.LoggerUtil;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SocketServerHandler implements Runnable {
    private final Logger LOGGER = LoggerFactory.getLogger(SocketServerHandler.class);
    private Socket socket;
    private Store store;
    private Map<ActionTypeEnum, ActionHandler> actionHandlers;

    public SocketServerHandler(Socket socket, Store store) {
        this.socket = socket;
        this.store = store;

        // 初始化命令处理器
        Map<ActionTypeEnum, ActionHandler> actionHandlers = new HashMap<>();
        actionHandlers.put(ActionTypeEnum.GET, new GetActionHandler());
        actionHandlers.put(ActionTypeEnum.SET, new SetActionHandler());
        actionHandlers.put(ActionTypeEnum.RM, new RmActionHandler());
    }

    @Override
    public void run() {
        ActionHandlerFactory handlerFactory = new ActionHandlerFactory();

        try (ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
             ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream())) {

            // 接收序列化对象
            Object receivedObject = ois.readObject();

            if (receivedObject instanceof ActionDTO) {
                ActionDTO dto = (ActionDTO) receivedObject;
                LoggerUtil.debug(LOGGER, "[SocketServerHandler][ActionDTO]: {}", dto.toString());

                // 获取适当的处理器并处理动作
                ActionHandler handler = handlerFactory.getHandler(dto.getType());

                if (handler != null) {
                    handler.handle(dto, oos, store);
                } else {

                }
            } else {
                LoggerUtil.warn(LOGGER, "Received object is not of type ActionDTO");
            }
        } catch (IOException | ClassNotFoundException e) {
            LoggerUtil.error(LOGGER, e,"Exception handling socket connection: {}", e.getMessage());
        } finally {
            try {
                socket.close();
            } catch (IOException e) {
                LoggerUtil.error(LOGGER, e,"Error closing socket: {}", e.getMessage());
            }
        }
    }
}
