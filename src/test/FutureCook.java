import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by gaojian on 2018/9/3.
 */
public class FutureCook {

    @Test
    public void test() {
        long startTime = System.currentTimeMillis();
        // 第一步 网购厨具
        Callable<Chuju> onlineShopping = new Callable<Chuju>() {

            @Override
            public Chuju call() throws Exception {
                System.out.println("第一步：下单");
                System.out.println("第一步：等待送货");
                Thread.sleep(10000);  // 模拟送货时间
                System.out.println("第一步：快递送到");
                return new Chuju();
            }

        };
        FutureTask<Chuju> task = new FutureTask<Chuju>(onlineShopping);
        new Thread(task).start();
        // 第二步 去超市购买食材
        try {
            Thread.sleep(2000);  // 模拟购买食材时间
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Shicai shicai = new Shicai();
        System.out.println("第二步：食材到位");
        // 第三步 用厨具烹饪食材
        if (!task.isDone()) {  // 联系快递员，询问是否到货
            System.out.println("第三步：厨具还没到，心情好就等着（心情不好就调用cancel方法取消订单）");
        }
        Chuju chuju = null;
        try {
            chuju = task.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        System.out.println("第三步：厨具到位，开始展现厨艺");
        cook(chuju, shicai);

        System.out.println("总共用时" + (System.currentTimeMillis() - startTime) + "ms");
    }

    //  用厨具烹饪食材
    static void cook(Chuju chuju, Shicai shicai) {}

    // 厨具类
    static class Chuju {}

    // 食材类
    static class Shicai {}


    @Test
    public void test2(){
        List<Integer> wrappedMessagesToHandle = new CopyOnWriteArrayList<>();

        int size = 100;
        ForkJoinPool forkJoinPool = new ForkJoinPool();
        forkJoinPool.invoke(new TempRecursiveTask(wrappedMessagesToHandle, 0, size));

        System.out.println(wrappedMessagesToHandle.size());

    }

    private class TempRecursiveTask extends RecursiveAction {

        private List<Integer> wrappedMessagesToHandle;
        private int start;
        private int end;

        public TempRecursiveTask(List<Integer> wrappedMessagesToHandle, int start, int end){
            this.wrappedMessagesToHandle = wrappedMessagesToHandle;
            this.start = start;
            this.end = end;
        }

        @Override
        protected void compute() {
            if(end - start <= 5){
                for(int i = start; i < end; i++){
                    wrappedMessagesToHandle.add(new Integer(i));
                }

                try {
                    Thread.sleep(300L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                int mid = (start + end) / 2;
                TempRecursiveTask leftTask = new TempRecursiveTask(wrappedMessagesToHandle, start, mid);
                TempRecursiveTask rightTask = new TempRecursiveTask(wrappedMessagesToHandle, mid, end);

                leftTask.fork();
                rightTask.fork();

                leftTask.join();
                rightTask.join();
            }
        }
    }

    @Test
    public void test3(){
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

    }


}
