package utils;

import java.io.*;

public class StatefulClass {
    private int currentState;
    String filePath = "state.dat";
    // 将状态保存到文件中
    public void saveStateToFile(String filePath) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            oos.writeInt(currentState);
            System.out.println("State saved successfully.");
        } catch (IOException e) {
            System.err.println("Failed to save state: " + e.getMessage());
        }
    }

    // 从文件中恢复状态
    public void restoreStateFromFile(String filePath) {
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filePath))) {
            currentState = ois.readInt();
            System.out.println("State restored successfully.");
        } catch (IOException e) {
            System.err.println("Failed to restore state: " + e.getMessage());
        }
    }


}

