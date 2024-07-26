/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import controller.SocketServerHandler;
//import dto.LogFileHandler;
import dto.WALEntry;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.*;

import java.util.Map;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import com.alibaba.fastjson.JSON;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.jar.JarEntry;

public class NormalStore implements Store {

    public static final String TABLE = ".table";
    public static final String RW_MODE = "rw";
    public static final String NAME = "data";
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    private final String logFormat = "[NormalStore][{}]: {}";
    private StatefulClass statefulObj;
    private TextFileDuplicateRemover textFileDuplicateRemover;
    String directoryToScan = "F:\\java高级编程实验\\easy-db-main\\data"; // 要扫描的目录路径
    private MultiThreadedRotateFileCompressor multiThreadedRotateFileCompressor;
    private RandomAccessFile walFile;
    // 写入磁盘的阈值大小
    private MemoryTable memoryTable ;  // 假设构造函数需要传入阈值


    /**
     * 内存表，类似缓存
     */
    private TreeMap<String, String> memTable;

    /**
     * hash索引，存的是数据长度和偏移量
     * */
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值
     */
//    private final int storeThreshold;

    public NormalStore(String dataDir) throws FileNotFoundException {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memoryTable= new MemoryTable(10,dataDir);
        this.index = new HashMap<>();
        this.statefulObj = new StatefulClass();  // 初始化 StatefulClass 实例
        this.statefulObj.restoreStateFromFile(dataDir + "/state.dat");
        this.textFileDuplicateRemover= new TextFileDuplicateRemover(directoryToScan);
        textFileDuplicateRemover.scheduleTask(0, 5*60000); // 每5分种运行一次// 恢复状态
        this.multiThreadedRotateFileCompressor=new MultiThreadedRotateFileCompressor();
        multiThreadedRotateFileCompressor.compressRotateFiles();
        this.walFile=new RandomAccessFile(this.dataDir+File.separator+"wal.log",RW_MODE);
        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER,logFormat, "NormalStore","dataDir isn't exist,creating...");
            file.mkdirs();
        }
        this.reloadIndex();
        this.replayLog();
    }

    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    public void reloadIndex() {
        try {
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        LoggerUtil.debug(LOGGER, logFormat, "reload index: "+index.toString());
    }

    @Override
    public void set(String key, String value) {
        logToWAL("set",key,value);
        try {
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = JSONObject.toJSONBytes(command);
            // 加锁
            indexLock.writeLock().lock();
            // TODO://先写内存表，内存表达到一定阀值再写进磁盘
            memoryTable.put(key, value);

            // 写table（wal）文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);

            // 添加索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            // 在写入磁盘之前，保存状态
            this.statefulObj.saveStateToFile(this.dataDir + "/state.dat");
            // 判断是否需要写入磁盘
            if (memoryTable.shouldFlushToDisk()) {
                memoryTable.flushToDisk();
                // 打印写入磁盘的文件路径
                System.out.println("数据已写入磁盘，文件路径：" + memoryTable.genDiskFilePath());
            }
            // TODO://判断是否需要将内存表中的值写回table
            if (memoryTable.shouldFlushToDisk()) {
                // 清空内存表
                memoryTable.clear();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.writeLock().unlock();
        }
    }


    @Override
    public String get(String key) {
        try {
            indexLock.readLock().lock();
            // 从索引中获取信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());

            JSONObject value = JSONObject.parseObject(new String(commandBytes));
            Command cmd = CommandUtil.jsonToCommand(value);
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            indexLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void rm(String key) {
        logToWAL("rm",key,null);
        try {
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // 加锁
            indexLock.writeLock().lock();
            try {
                // TODO: 先写内存表，内存表达到一定阀值再写进磁盘
                memoryTable.put(key, null); // 在内存表中标记要删除的键为null，或者采用其他方式标记删除

                RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
                int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);

                // 保存到内存表
                CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
                index.put(key, cmdPos);
                // 在写入磁盘之前，保存状态
                this.statefulObj.saveStateToFile(this.dataDir + "/state.dat");
                // 判断内存表是否达到写入磁盘的阈值
                if (memoryTable.shouldFlushToDisk()) {
                    memoryTable.flushToDisk();
                }
                // 再次保存状态，确保最新的状态被持久化
                this.statefulObj.saveStateToFile(this.dataDir + "/state.dat");
                // TODO: 判断是否需要将内存表中的值写回table
                if (memoryTable.shouldFlushToDisk()) {
                    // 清空内存表
                    memoryTable.clear();
                }

            } finally {
                indexLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }


    @Override
    public void close() throws IOException {

    }

    //将操作写入 WAL日志文件中
    private void logToWAL(String commandType, String key, String value) {
        try {
            // WALEntry包含三个属性：命令、key、value
            WALEntry entry = new WALEntry(commandType, key, value);
            byte[] entryBytes = JSONObject.toJSONBytes(entry);
            walFile.writeInt(entryBytes.length);
            walFile.write(entryBytes);
            walFile.getFD().sync();   //每次写入 WAL文件的数据及时刷新到磁盘
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //逐一回放日志文件
    public void replayLog() {
        try {
            walFile.seek(0);   //将文件指针移动到文件开头
            while (walFile.getFilePointer() < walFile.length()) {
                int entryLen = walFile.readInt();
                byte[] entryBytes = new byte[entryLen];
                walFile.read(entryBytes);
                WALEntry entry = JSONObject.parseObject(new String(entryBytes), WALEntry.class);
                //根据操作类型执行对应的操作
                if ("set".equals(entry.getCommandType())) {
                    set(entry.getKey(), entry.getValue());
                } else if ("rm".equals(entry.getCommandType())) {
                    rm(entry.getKey());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
