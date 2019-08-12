import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author: 楚森
 * @Description:
 * @Company: 枣庄学院
 * @Date: 2019/8/10/010 16:14
 * @Version: 1.0
 */
public class CalculatedCharacter {
    private static CountDownLatch countDownLatch;

    private static class ExecuteThread implements Runnable {
        private long fileSize;
        private final Map<Character, Long> hashMap;
        private long pos;
        private File file;

        ExecuteThread(long fileSize, long pos, File file, Map<Character, Long> currentHashMap) {
            this.fileSize = fileSize;
            this.pos = pos;
            this.hashMap = currentHashMap;
            this.file = file;
        }

        @Override
        public void run() {
//            System.out.println("---创建任务了，" + Thread.currentThread().getName() + "进入运行---");
            RandomAccessFile randomAccessFile;
            try {
                randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(pos);
                byte[] buf = new byte[(int) fileSize];
                int length = randomAccessFile.read(buf);
                char[] chars = new String(buf, 0, length).toCharArray();
                synchronized (hashMap) {
                    for (char cr : chars) {
                        if (cr == '\r' || cr == '\n') {
                            continue;
                        }
                        // 保证线程安全

                        Long num = hashMap.get(cr);
                        if (num == null) {
                            hashMap.put(cr, 1L);
                        } else {
                            hashMap.put(cr, num + 1);
                        }
                    }
                }
                countDownLatch.countDown();
                randomAccessFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
//            System.out.println(Thread.currentThread().getName() + ":任务运行结束");
        }
    }


    /**
     * 多线程处理读文件然后进行计算字符个数
     *
     * @param path
     * @param num
     */
    private static Map<Character, Long> multithreading(String path, int num) {
        Map<Character, Long> hashMap = new HashMap<>();
        File file = new File(path);
        long length = file.length();
        long fileSize = length / num;
//        System.out.println("fileSize: " + fileSize);
        countDownLatch = new CountDownLatch(num);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(num, num, 0,
                TimeUnit.SECONDS, new SynchronousQueue<>());
        for (int i = 0; i < num; i++) {
            if (i == num - 1) {
                executor.execute(new ExecuteThread(length - i * fileSize, i * fileSize, file, hashMap));
            } else {
                executor.execute(new ExecuteThread(fileSize, i * fileSize, file, hashMap));
            }
        }
        executor.shutdown();
        return hashMap;
    }

    private static Map<Character, Long> singleThread(String path) throws Exception {
        File file = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(file));
        Map<Character, Long> map = new HashMap<>();
        String line = null;
        while ((line = br.readLine()) != null) {
            char[] chars = line.toCharArray();
            for (char c : chars) {
                Long num = map.get(c);
                if (num == null) {
                    map.put(c, 1L);
                } else {
                    map.put(c, num + 1);
                }
            }
        }
        br.close();
        return map;
    }

    private static void printRes(Map<Character, Long> map) {
        System.out.println("map size: " + map.size());
        StringBuilder sb1 = new StringBuilder();
        map.forEach((key, value) -> sb1.append(key).append("_").append(value).append(","));
        System.out.println(sb1.deleteCharAt(sb1.toString().length() - 1));
    }

    public static void main(String[] args) throws Exception {
        String path = "D:\\test\\test.data";
        System.out.println("File length: " + ((new File(path).length()) / 1024 / 1024) + "MB");
        System.out.println("----------------------多线程版本(全局Map方式)------------------------");
        long startTime = System.currentTimeMillis();
        Map<Character, Long> map = multithreading(path, 4);
        countDownLatch.await();
        System.out.println("多线程耗时: " + (System.currentTimeMillis() - startTime) + "ms");
        printRes(map);
        System.out.println("----------------------单线程版本------------------------");
        // 单线程版本
        long startTime1 = System.currentTimeMillis();
        Map<Character, Long> singleMap = singleThread(path);
        System.out.println("单线程耗时: " + (System.currentTimeMillis() - startTime1) + "ms");
        printRes(singleMap);


    }

}
