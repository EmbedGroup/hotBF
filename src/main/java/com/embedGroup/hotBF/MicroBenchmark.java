package com.embedGroup.hotBF;

import java.io.File;

import org.iota.jota.utils.SeedRandomGenerator;

public class MicroBenchmark {
    public static void Size() {
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 4 * 1024 * 8 * 1024);
        hot.ShutDown();
    }

    public static void correctness() {
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 4 * 1024 * 8 * 1024);

        hot.Insert("99999");
        if (hot.mayExists("99999")) {
            System.out.println("exists");
        }
        // first block,all active
        hot.ShutDown();
    }

    public static void clear() {
        File f = new File("HBF");
        if (f.exists())
            f.delete();
        f = new File("HBF-Meta");
        if (f.exists())
            f.delete();
        try {
            Runtime tun = Runtime.getRuntime();
            Process pro = tun.exec("./del.sh");
            int status=pro.waitFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void LRUtest() {
         clear();
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 4 * 1024 * 8 * 6);// 3 BFUs allwed in memory
        hot.Insert("99999");// load in 5 BFUs
        hot.Insert("99A99");// load new 5BFUs,Eliminate block0,BFU0 1 2 3

        // blockLRU2.3.4
        // active block0 BFU5,block1,BFU0 1 2 3 4

        if (hot.mayExists("99999")) {
            System.out.println("Exists");
        } // no need to load

        if (hot.mayExists("99A99")) {
            System.out.println("Exists");
        } // no need to load

        if (hot.mayExists("99B9")) {
            System.out.println("Exists");
        } // no need to load

        hot.ShutDown();
    }

    public static void dencyTest() {
        clear();
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 4 * 1024 * 8 * 1024);
        int amount = 100;
        String[] addresses = new String[amount];
        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();
            addresses[i] = addr;
            hot.Insert(addr);
        }
        System.out.println("test");
        int amount2 = 100;
        for (int i = 0; i < amount2; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();
            if (hot.mayExists(addr))
                System.out.println("Flase Positive");
        }

        hot.ShutDown();
    }

    public static void ScaleTest(){
        clear();
        HotBF hot=new HotBF();
        hot.ini(3, 20, 2, 0.001, 1024*8*1024*100);//capacity=6
        for(int i=0;i<7;i++){
            hot.Insert("9999"+i);
        }
        hot.ShutDown();

        hot.ini(3, 20, 2, 0.001, 1024*8*1024*100);//capacity=6
        for(int i=0;i<7;i++){
            if(!hot.mayExists("9999"+i)){
                System.out.println("miss inserted");
            }
        }

        for(int i=0;i<100;i++){
            if(hot.mayExists("9999"+(8+i))){
                System.out.println("false positive");
            }
        }

        hot.ShutDown();
    }
}