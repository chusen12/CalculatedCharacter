import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author: 楚森
 * @Description:
 * @Company: 枣庄学院
 * @Date: 2019/8/10/010 16:14
 * @Version: 1.0
 */
public class CalculatedCharacter1 {
    private static CountDownLatch countDownLatch;

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
//            System.out.println("---创建任务了，"+Thread.currentThread().getName()+"进入运行---");
            RandomAccessFile randomAccessFile;
            HashMap<Character, Long> map = new HashMap<>();
            try {

                randomAccessFile = new RandomAccessFile(file, "r");
                randomAccessFile.seek(pos);
                byte[] buf = new byte[(int) fileSize];
//                System.out.println(buf.length);
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

                countDownLatch.countDown();
                randomAccessFile.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
//            System.out.println(Thread.currentThread().getName()+"：任务运行结束");
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
        countDownLatch = new CountDownLatch(num);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(num, num, 0,
                TimeUnit.SECONDS, new SynchronousQueue<>());
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
            mergeMap(map,subMap);
        }
        executor.shutdown();
        return map;

    }

    private static void mergeMap(Map<Character, Long> map, HashMap<Character, Long> subMap) {
        for (Map.Entry<Character, Long> sub : subMap.entrySet()) {
            map.merge(sub.getKey(), sub.getValue(), (a, b) -> a + b);
        }
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
