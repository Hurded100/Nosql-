/*
 *@Type CmdClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;

import java.util.Scanner;

public class CmdClient {
    private Client client;

    public CmdClient(Client client) {
        this.client = client;
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;

        while (running) {
            System.out.print("请输入命令 (set/get/rm/quit): ");
            String command = scanner.nextLine();

            switch (command) {
                case "set":
                    handleSetCommand(scanner);
                    break;
                case "get":
                    handleGetCommand(scanner);
                    break;
                case "rm":
                    handleRmCommand(scanner);
                    break;
                case "quit":
                    running = false;
                    break;
                default:
                    System.out.println("无效命令，请重试。");
            }
        }

        scanner.close();
    }

    private void handleSetCommand(Scanner scanner) {
        System.out.print("请输入键：");
        String key = scanner.nextLine();

        System.out.print("请输入值：");
        String value = scanner.nextLine();

        client.set(key, value);
    }

    private void handleGetCommand(Scanner scanner) {
        System.out.print("请输入键：");
        String key = scanner.nextLine();

       client.get(key);
        //System.out.println("值：" + value);
    }

    private void handleRmCommand(Scanner scanner) {
        System.out.print("请输入键：");
        String key = scanner.nextLine();

        client.rm(key);
    }

}