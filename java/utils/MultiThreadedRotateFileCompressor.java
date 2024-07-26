package utils;

import java.io.*;
import java.util.concurrent.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class MultiThreadedRotateFileCompressor {

    private static final int THREAD_COUNT = 4; // 线程数量
    private static final String LOG_FILES_DIRECTORY = "data"; // Rotate文件存放目录
    private static final String COMPRESSED_FILES_DIRECTORY = "data"; // 压缩文件存放目录
    private static final long MAX_DIRECTORY_SIZE = 1000 * 1024 * 1024; // 最大目录大小，单位为字节

    public void compressRotateFiles() {
        // 创建固定大小的线程池
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        // 获取Rotate文件列表
        File logDirectory = new File(LOG_FILES_DIRECTORY);
        File[] txtFiles = logDirectory.listFiles((dir, name) -> name.toLowerCase().endsWith(".txt"));

        long totalDirectorySize = calculateDirectorySize(logDirectory);

        if (txtFiles != null) {
            // 提交每个Rotate文件的压缩任务给线程池
            for (File file : txtFiles) {
                long fileSize = file.length();
                totalDirectorySize += fileSize;

                if (totalDirectorySize >= MAX_DIRECTORY_SIZE) {
                    System.out.println("目录大小达到阈值，开始压缩文件。");
                    Runnable task = new CompressTask(file, COMPRESSED_FILES_DIRECTORY);
                    executor.submit(task);
                    totalDirectorySize = 0; // 重置目录大小计数器
                }
            }
        }

        // 关闭线程池
        executor.shutdown();

        try {
            // 等待所有任务完成
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("所有Rotate文件已压缩完成！");
    }

    private long calculateDirectorySize(File directory) {
        long totalSize = 0;
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile()) {
                    totalSize += file.length();
                }
            }
        }
        return totalSize;
    }

    private static class CompressTask implements Runnable {

        private File fileToCompress;
        private String destinationDirectory;

        public CompressTask(File fileToCompress, String destinationDirectory) {
            this.fileToCompress = fileToCompress;
            this.destinationDirectory = destinationDirectory;
        }

        @Override
        public void run() {
            try {
                // 模拟压缩Rotate文件的操作
                System.out.println("正在压缩文件: " + fileToCompress.getName());
                compressFile(fileToCompress, destinationDirectory);
                System.out.println("压缩完成: " + fileToCompress.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void compressFile(File fileToCompress, String destinationDirectory) throws IOException {
            // 使用ZipOutputStream压缩文件
            String zipFileName = destinationDirectory + File.separator + fileToCompress.getName() + ".zip";
            try (FileOutputStream fos = new FileOutputStream(zipFileName);
                 ZipOutputStream zos = new ZipOutputStream(fos)) {

                // 将文件写入ZipOutputStream
                try (FileInputStream fis = new FileInputStream(fileToCompress)) {
                    ZipEntry zipEntry = new ZipEntry(fileToCompress.getName());
                    zos.putNextEntry(zipEntry);

                    byte[] bytes = new byte[1024];
                    int length;
                    while ((length = fis.read(bytes)) >= 0) {
                        zos.write(bytes, 0, length);
                    }
                    zos.closeEntry();
                }
            }
        }
    }
}
