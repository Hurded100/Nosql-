package dto;

// 动作处理器工厂
public class ActionHandlerFactory {
    public ActionHandler getHandler(ActionTypeEnum type) {
        switch (type) {
            case GET:
                return new GetActionHandler();
            case SET:
                return new SetActionHandler();
            case RM:
                return new RmActionHandler();
            default:
                return null;
        }
    }
}