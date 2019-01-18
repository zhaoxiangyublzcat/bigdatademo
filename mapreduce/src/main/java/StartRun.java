import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class StartRun {
    public static Map<String, Integer> R = new HashMap<>();
    static Map<String, String> paths;
    static Configuration conf;


    public static void main(String[] args) {
        R.put("click", 1);
        R.put("cart", 2);
        R.put("bug", 3);

        conf = new Configuration();
        paths = new HashMap<>();
//        paths.put("Step1Input", "/root/data/recommendIn.txt");
//        paths.put("Step1Output", "/root/data/recommendMR/Step1Output");
//        paths.put("Step2Input", paths.get("Step1Output"));
//        paths.put("Step2Output", "/root/data/recommendMR/Step2Output");
//        paths.put("Step3Input1", paths.get("Step2Output"));
//        paths.put("Step3Input2", paths.get("Step3Output"));
//        paths.put("Step3Output", "/root/data/recommendMR/Step3Output");
//        paths.put("Step4Input", paths.get("Step3Output"));
//        paths.put("Step4Output", "/root/data/recommendMR/Step4Output");
//        paths.put("Step5Input", paths.get("Step4Output"));
//        paths.put("Step5Output", "/root/data/recommendMR/Step5Output");



        paths.put("Step1Input", "/home/hadoop/data/recommendIn.txt");
        paths.put("Step1Output", "/home/hadoop/data/recommendMR/Step1Output");
        paths.put("Step2Input", paths.get("Step1Output"));
        paths.put("Step2Output", "/home/hadoop/data/recommendMR/Step2Output");
        paths.put("Step3Input1", paths.get("Step2Output"));
        paths.put("Step3Input2", paths.get("Step3Output"));
        paths.put("Step3Output", "/home/hadoop/data/recommendMR/Step3Output");
        paths.put("Step4Input", paths.get("Step3Output"));
        paths.put("Step4Output", "/home/hadoop/data/recommendMR/Step4Output");
        paths.put("Step5Input", paths.get("Step4Output"));
        paths.put("Step5Output", "/home/hadoop/data/recommendMR/Step5Output");



        MyThread1 t1 = new MyThread1();
        MyThread1 t2 = new MyThread1();
        MyThread1 t3 = new MyThread1();
        MyThread1 t4 = new MyThread1();
        MyThread1 t5 = new MyThread1();

        try {
            t1.start();
            t1.join();
            t2.start();
            t2.join();
            t3.start();
            t3.join();
            t4.start();
            t4.join();
            t5.start();
            t5.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static class MyThread1 extends Thread {
        @Override
        public void run() {
            Step1.run(conf, paths);
        }
    }

    public static class MyThread2 extends Thread {
        @Override
        public void run() {
            Step2.run(conf, paths);
        }
    }

    public static class MyThread3 extends Thread {
        @Override
        public void run() {
            Step3.run(conf, paths);
        }
    }

    public static class MyThread4 extends Thread {
        @Override
        public void run() {
            Step4.run(conf, paths);
        }
    }

    public static class MyThread5 extends Thread {
        @Override
        public void run() {
            Step5.run(conf, paths);
        }
    }
}
