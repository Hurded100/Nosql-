package utils;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TextFileDuplicateRemover {
    private String directory; // 要扫描的目录路径
    private Map<String, String> fileData; // 存储文件内容和路径的映射表

    public TextFileDuplicateRemover(String directory) {
        this.directory = directory;
        this.fileData = new HashMap<>();
    }

    // 扫描指定目录下的文本文件
    public void scanDirectory() {
        try {
            Files.walkFileTree(Paths.get(directory), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (attrs.isRegularFile() && file.toString().toLowerCase().endsWith(".txt")) {
                        try {
                            String fileContent = readFileContent(file.toFile());
                            fileData.put(file.toString(), fileContent); // 存储文件路径和内容的映射
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    System.err.println("Failed to visit file: " + file.toString() + " " + exc.getMessage());
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 读取文件内容并返回字符串
    private String readFileContent(File file) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString();
    }

    public void removeDuplicates() {
        // 反转映射表，以文件内容作为键来查找重复的路径并保留最新的
        Map<String, String> uniqueFiles = new HashMap<>();

        for (Map.Entry<String, String> entry : fileData.entrySet()) {
            String filePath = entry.getKey();
            String content = entry.getValue();

            if (!uniqueFiles.containsKey(content)) {
                uniqueFiles.put(content, filePath);
            } else {
                // 如果已经存在相同内容的文件，则比较文件的最后修改时间，保留最新的文件路径
                String existingFilePath = uniqueFiles.get(content);
                File existingFile = new File(existingFilePath);
                File currentFile = new File(filePath);
                if (currentFile.lastModified() > existingFile.lastModified()) {
                    uniqueFiles.put(content, filePath);
                }
            }
        }

        // 更新为唯一文件路径映射表（这里更新的是 fileData）
        fileData.clear();
        for (Map.Entry<String, String> entry : uniqueFiles.entrySet()) {
            fileData.put(entry.getValue(), entry.getKey());
        }
    }

//    // 打印剩余的唯一文件信息
public void printFileData() {
    System.out.println("唯一文本文件：");
    for (String fileName : fileData.keySet()) {
        System.out.println(fileName);
    }
}


    // 将扫描、删除重复文件和打印的操作封装到一个方法中，用于定时任务调用
    public void runDuplicateRemover() {
        scanDirectory(); // 开始扫描目录
        removeDuplicates(); // 删除重复文件
    }

    // 添加定时任务调度方法
    public void scheduleTask(long initialDelay, long period) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(this::runDuplicateRemover, initialDelay, period, TimeUnit.MILLISECONDS);
    }
    public static void main(String[] args) {
        String directoryToScan = "F:\\java高级编程实验\\easy-db-main\\data"; // 要扫描的目录路径
        TextFileDuplicateRemover remover = new TextFileDuplicateRemover(directoryToScan);

        // 测试定时任务
        remover.scheduleTask(0, 60000); // 每60秒运行一次

        // 等待一段时间以让定时任务运行
        try {
            Thread.sleep(30000); // 等待30秒
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 打印剩余的唯一文件信息
        remover.printFileData();
    }

}
