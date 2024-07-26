package dto;

import service.Store;

import java.io.IOException;
import java.io.ObjectOutputStream;

// ActionHandler.java
public interface ActionHandler {
    void handle(ActionDTO dto, ObjectOutputStream oos, Store store) throws IOException;
}

