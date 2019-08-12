import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author: 楚森
 * @Description:解决单机跑大文件OOM异常
 * @Company: 枣庄学院
 * @Date: 2019/8/10/010 16:14
 * @Version: 1.0
 */
public class CalculatedCharacter2 {
    private static CountDownLatch countDownLatch;

    private static class ExecuteThread implements Runnable {
        private long fileSize;
        private final Map<Character, Long> concurrentHashMap;
        private long pos;
        private File file;

        ExecuteThread(long fileSize, long pos, File file, Map<Character, Long> currentHashMap) {
            this.fileSize = fileSize;
            this.pos = pos;
            this.concurrentHashMap = currentHashMap;
            this.file = file;
        }
        @Override
        public void run() {
            System.out.println("---创建任务了，" + Thread.currentThread().getName() + "进入运行---");
            RandomAccessFile randomAccessFile;
            try {
                randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(pos);
                byte[] buf;
                int bufSize = 1024 * 15;
                long bufNum = fileSize / bufSize;
                for (long i = 0; i < bufNum + 1; i++) {
                    if (fileSize % bufSize == 0 && i == bufNum) {
                        break;
                    }
                    buf = fileSize % bufSize != 0 && i == bufNum ? new byte[(int) (fileSize - i * bufSize)] : new byte[bufSize];
                    int length = randomAccessFile.read(buf);
                    char[] chars = new String(buf, 0, length).toCharArray();
                    synchronized (concurrentHashMap) {
                        for (char cr : chars) {
                            if (cr == '\r' || cr == '\n') {
                                continue;
                            }
                            Long num = concurrentHashMap.get(cr);
                            if (num == null) {
                                concurrentHashMap.put(cr, 1L);
                            } else {
                                concurrentHashMap.put(cr, num + 1);
                            }
                        }
                    }
                }

                countDownLatch.countDown();
                randomAccessFile.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + ":任务运行结束");
        }
    }


    /**
     * 多线程处理读文件然后进行计算字符个数
     *
     * @param path
     * @param num
     */
    private static Map<Character, Long> multithreading(String path, int num) {
        Map<Character, Long> concurrentHashMap = new HashMap<>();
        File file = new File(path);
        long length = file.length();
        long fileSize = length / num;
        System.out.println("fileSize: " + fileSize);
        countDownLatch = new CountDownLatch(num);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(num, num, 10,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(20));
        for (int i = 0; i < num; i++) {
            if (i == num - 1) {
                executor.execute(new ExecuteThread(length - i * fileSize, i * fileSize, file, concurrentHashMap));
            } else {
                executor.execute(new ExecuteThread(fileSize, i * fileSize, file, concurrentHashMap));
            }
        }
        executor.shutdown();
        return concurrentHashMap;

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
        Thread.sleep(15000);
        String path = "D:\\test\\test.dat";
        System.out.println("File length: " + (new File(path).length()));
        System.out.println("----------------------多线程版本(全局Map方式)------------------------");
        long startTime = System.currentTimeMillis();
        Map<Character, Long> map = multithreading(path, 30);
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
