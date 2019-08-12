import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author: 楚森
 * @Description: 解决单机跑大文件OOM异常
 * @Company: 枣庄学院
 * @Date: 2019/8/10/010 16:14
 * @Version: 1.0
 */
public class CalculatedCharacter3 {
    private static CountDownLatch countDownLatch;

    private static class TaskThread extends RecursiveTask<Map<Character, Long>> {
        private static final long serialVersionUID = -518192784406917765L;
        private long end;
        private long pos;
        private File file;

        public TaskThread(File file, long pos, long end) {
            this.file = file;
            this.end = end;
            this.pos = pos;
        }

        @Override
        protected Map<Character, Long> compute() {
            int bufSize = 1024 * 60;
            RandomAccessFile randomAccessFile;
            Map<Character, Long> map = new HashMap<>();
            try {
                randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(pos);
                long fileSize = end - pos;
                if (end - pos <= 20 * 1024) {
                    byte[] buf;
                    long bufNum = fileSize / bufSize;
                    for (long i = 0; i < bufNum + 1; i++) {
                        buf = fileSize % bufSize != 0 && i == bufNum ? new byte[(int) (fileSize - i * bufSize)] : new byte[bufSize];
                        int length = randomAccessFile.read(buf);
                        char[] chars = new String(buf, 0, length).toCharArray();
                        for (char cr : chars) {
                            if (cr == '\r' || cr == '\n') {
                                continue;
                            }
                            Long num = map.get(cr);
                            if (num == null) {
                                map.put(cr, 1L);
                            } else {
                                map.put(cr, num + 1);
                            }
                        }
                    }
                    randomAccessFile.close();
                } else {
                    long mid = (pos + end) / 2;
                    TaskThread left = new TaskThread(file, pos, mid);
                    left.fork();
                    TaskThread right = new TaskThread(file, mid, end);
                    right.fork();
                    return mergeMap(left.join(), right.join());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
            return map;

//            if (to - from <= THRESHOLD) {
//                for (int i = from; i < to; i++) {
//                    if (riceArray[i] == 1) {
//                        total += 1;
//                    }
//                }
//                return total;
//            } else {
//                int mid = (from + to) / 2;
//                TogetherCounter.CounterTask left = new TogetherCounter.CounterTask(riceArray, from, mid);
//                left.fork();
//                TogetherCounter.CounterTask right = new TogetherCounter.CounterTask(riceArray, mid + 1, to);
//                right.fork();
//                return left.join() + right.join();
//            }
        }
    }

    private static class ExecuteThread implements Callable<HashMap<Character, Long>> {
        private long fileSize;
        private long pos;
        private File file;

        ExecuteThread(long fileSize, long pos, File file) {
            this.fileSize = fileSize;
            this.pos = pos;
            this.file = file;
        }


        @Override
        public HashMap<Character, Long> call() throws Exception {
        //    System.out.println("---创建任务了，" + Thread.currentThread().getName() + "进入运行---");
            RandomAccessFile randomAccessFile;
            HashMap<Character, Long> map = new HashMap<>();
            try {

                randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(pos);
                byte[] buf;
                int bufSize = 1024 * 100;
        //        System.out.println(fileSize);
                long bufNum = fileSize / bufSize;
                for (long i = 0; i < bufNum + 1; i++) {
                    if (fileSize % bufSize == 0 && i == bufNum) {
                        break;
                    }
                    buf = fileSize % bufSize != 0 && i == bufNum ? new byte[(int) (fileSize - i * bufSize)] : new byte[bufSize];
                    int length = randomAccessFile.read(buf);
                    char[] chars = new String(buf, 0, length).toCharArray();
                    for (char cr : chars) {
                        if (cr == '\r' || cr == '\n') {
                            continue;
                        }
                        Long num = map.get(cr);
                        if (num == null) {
                            map.put(cr, 1L);
                        } else {
                            map.put(cr, num + 1);
                        }
                    }
                }
                countDownLatch.countDown();
                randomAccessFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        //    System.out.println(Thread.currentThread().getName() + "：任务运行结束");
            return map;
        }
    }

    private static void generateData() throws IOException {
        File file = new File("d:/test/test.dat");
        char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
        };
        FileWriter fileWriter = new FileWriter(file, true);
        BufferedWriter bw = new BufferedWriter(fileWriter);
        Random random = new Random();
        for (int i = 0; i < 10000_00000; i++) {
            bw.write(chars[random.nextInt(chars.length)]);
            if (i % 200 == 0) {
                bw.write(System.lineSeparator());
            }
        }
        bw.flush();
        fileWriter.close();

    }


    /**
     * 多线程处理读文件然后进行计算字符个数
     *
     * @param path
     * @param num
     */
    private static Map<Character, Long> multithreading(String path, int num) throws Exception {
        Map<Character, Long> map = new HashMap<>();
        File file = new File(path);
        long length = file.length();
        long fileSize = length / num;
//        System.out.println("fileSize: " + fileSize);
        countDownLatch = new CountDownLatch(num);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, num, 0,
                TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        // 直接创建核心线程
//        executor.prestartAllCoreThreads();
        Future<HashMap<Character, Long>> future;
        List<Future<HashMap<Character, Long>>> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            if (i == num - 1) {
                future = executor.submit(new ExecuteThread(length - i * fileSize, i * fileSize, file));
            } else {
                future = executor.submit(new ExecuteThread(fileSize, i * fileSize, file));
            }
            list.add(future);
            //这个操作使得多线程变为了串行执行
//            HashMap<Character, Long> subMap = future.get();
//            mergeMap(map, subMap);
        }
        for (Future<HashMap<Character, Long>> hashMapFuture : list) {
            HashMap<Character, Long> subMap = hashMapFuture.get();
            mergeMap(map, subMap);
        }
        executor.shutdown();
        return map;

    }

    private static Map<Character, Long> mergeMap(Map<Character, Long> map, Map<Character, Long> subMap) {
        for (Map.Entry<Character, Long> sub : subMap.entrySet()) {
            map.merge(sub.getKey(), sub.getValue(), (a, b) -> a + b);
        }
        return map;
    }

    private static Map<Character, Long> forkTask(String path, int num) throws Exception {
        Map<Character, Long> map = new HashMap<>();
        Map<Character, Long> subMap;
        File file = new File(path);
        countDownLatch = new CountDownLatch(num);
        ForkJoinPool pool = new ForkJoinPool(num);
        long length = file.length();
        long fileSize = length / num;
        System.out.println("fileSize: " + fileSize);
        List<Map<Character, Long>> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            long pos = i * fileSize;
            if (i == num - 1) {
                long size = length - i * fileSize;
                subMap = pool.invoke(new TaskThread(file, pos, pos + size));
            } else {
                subMap = pool.invoke(new TaskThread(file, pos, pos + fileSize));
            }
            list.add(subMap);
        }
        for (Map<Character, Long> m : list) {
            mergeMap(map, m);
        }
        pool.shutdown();
        return map;
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
//        StringBuilder sb1 = new StringBuilder();
//        for (Map.Entry<Character, Long> characterLongEntry : map.entrySet()) {
//            sb1.append(characterLongEntry.getKey()).append("_").append(characterLongEntry.getValue()).append(",");
//        }
        System.out.println("map size: " + map.size());
        StringBuilder sb1 = new StringBuilder();
        map.forEach((key, value) -> sb1.append(key).append("_").append(value).append(","));
        System.out.println(sb1.deleteCharAt(sb1.toString().length() - 1));
    }

    public static void main(String[] args) throws Exception {
//        generateData();
//        Thread.sleep(15000);
        String path = "D:\\test\\test.dat";
        System.out.println("File length: " + ((new File(path).length()) / 1024 / 1024) + "MB");
        System.out.println("----------------------多线程版本(子线程创建Map返回)------------------------");
        long startTime = System.currentTimeMillis();
        Map<Character, Long> map = multithreading(path, 10);
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
