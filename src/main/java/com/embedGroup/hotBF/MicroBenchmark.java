package com.embedGroup.hotBF;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;
import com.embedGroup.hotBF.HotBF.micromonitor;

import org.iota.jota.pow.ICurl;
import org.iota.jota.pow.SpongeFactory;
import org.iota.jota.utils.IotaAPIUtils;
import org.iota.jota.utils.IotaUnits;
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
            int status = pro.waitFor();
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
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 8 * 1024);
        int amount = 1000;
        String[] addresses1 = new String[amount];
        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();
            addresses1[i] = addr;
            hot.Insert(addr);
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }

        for (int i = 0; i < amount; i++) {
            if (!hot.mayExists(addresses1[i])) {
                System.out.println("ERROR");
            }
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }

        hot.ShutDown();

        hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 8 * 1024);
        String[] addresses2 = new String[amount];
        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();
            addresses2[i] = addr;
            hot.Insert(addr);
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }

        for (int i = 0; i < amount; i++) {
            if (!hot.mayExists(addresses1[i])) {
                System.out.println("ERROR");
            }
            if (i % 100 == 0) {
                System.out.println(i);
            }
        }

        hot.ShutDown();

        hot.reporter.report();
        GroupBloomFilter.reporter.report();
        Buffer.reporter.report();
        
    }

    public static void ScaleTest() {
        clear();
        HotBF hot = new HotBF();
        hot.ini(3, 20, 2, 0.001, 1024 * 8 * 1024 * 100);// capacity=6
        for (int i = 0; i < 7; i++) {
            hot.Insert("9999" + i);
        }
        hot.ShutDown();

        hot.ini(3, 20, 2, 0.001, 1024 * 8 * 1024 * 100);// capacity=6
        for (int i = 0; i < 7; i++) {
            if (!hot.mayExists("9999" + i)) {
                System.out.println("miss inserted");
            }
        }

        for (int i = 0; i < 100; i++) {
            if (hot.mayExists("9999" + (8 + i))) {
                System.out.println("false positive");
            }
        }

        hot.ShutDown();
    }

    public static void randominsertLatency() {
        clear();

        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.MILLISECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(5, TimeUnit.SECONDS);
        Timer t = metrics.timer("Insert");
        HotBF hot = new HotBF();
        hot.ini(1, 4 * 1024 * 8, 2, 0.001, 10 * 1024 * 1024 * 8);
        int amount = 1000000;
        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();
            Timer.Context ctx = t.time();
            hot.Insert(addr);
            ctx.close();
        }

        reporter.report();
        hot.ShutDown();// 13ms
    }

    public static void randomCheckLatency() {
        // clear();

        MetricRegistry metrics = new MetricRegistry();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.MILLISECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(5, TimeUnit.SECONDS);
        Timer t2 = metrics.timer("check");
        Timer t1 = metrics.timer("Insert");
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        int amount = 1000;
        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();// 5.3ms
            Timer.Context ctx = t1.time();
            hot.Insert(addr);
            ctx.close();
        }

        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();// 2.2ms
            Timer.Context ctx = t2.time();
            hot.mayExists(addr);
            ctx.close();
        }

        hot.ShutDown();// 5ms

    }

    /**
     * check: checkBFUinmemory loadBFU eliminate
     */
    public static void checkbreakdown() {
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        int amount = 10000;
        for (int i = 0; i < amount; i++) {
            String addr = SeedRandomGenerator.generateNewSeed();
            hot.mayExists(addr);
            if (i % 1000 == 0) {
                System.out.println(i);
            }
        }
        System.out.println("load BFU times " + GroupBloomFilter.loadtimes);
        System.out.println("save BFU times " + GroupBloomFilter.savetimes);
        // Elininate 2ms,mayexists 9ms
        hot.ShutDown();
    }

    public static void batch() {
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        long start = System.currentTimeMillis();
        hot.ShutDown();
        System.out.println("Cost time: " + (System.currentTimeMillis() - start) + "ms");// 35.19s
    }

    public static void empty() {
        clear();
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        for (int i = 0; i < 10000; i++) {
            hot.mayExists(SeedRandomGenerator.generateNewSeed());
        }
        hot.ShutDown();
    }

    public static void ini_shutdown() {
        clear();
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        for (int i = 0; i < 27 * 27 * 27; i++) {
            hot.Insert(Utils.IntToTrytes(i, 3) + "AAAAAAAAAAAAAA");// activite all GBF's
        }
        hot.ShutDown();

        hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        for (int i = 0; i < 27 * 27 * 27; i++) {
            if (!hot.mayExists(Utils.IntToTrytes(i, 3) + "AAAAAAAAAAAAAA")) {
                System.out.println("error");
                // activite all GBF's
            }
        }
        hot.ShutDown();
    }

    public static void insight() {
        clear();
        HotBF hot = new HotBF();
        // raw bootstrap
        hot.ini(1, 4 * 1024 * 8, 2, 0.001, 4 * 1024 * 8 * 5 * 3);
        ICurl customCurl = SpongeFactory.create(SpongeFactory.Mode.KERL);
        String seed = "QV9BDEJQVJHRBVKZQKYIJBCQVMUZQTSKAGHF9CUXEZPVQKGMPYJSMLVGVXVQGMUINQIY9MOUABVYUGBMD";
        int amount = 1;
        // insert till full
        for (int i = 0; i < 3; i++) {
            String address = IotaAPIUtils.newAddress(seed, 1, i, false, customCurl);
            System.out.println(Utils.TrytesToInt(address, 1) + ":" + address);
            hot.Insert(address);
        }

        hot.print();

        // insert new one,elimanate old 5
        String address = IotaAPIUtils.newAddress(seed, 1, 4, false, customCurl);
        System.out.println(Utils.TrytesToInt(address, 1) + ":" + address);
        hot.Insert(address);
        hot.print();

        // check inserted and elimated,bring 5 in
        address = IotaAPIUtils.newAddress(seed, 1, 0, false, customCurl);
        System.out.println(Utils.TrytesToInt(address, 1) + ":" + address);
        hot.mayExists(address);
        hot.print();

        // check one not inserted,bring 1 in
        address = IotaAPIUtils.newAddress(seed, 1, 5, false, customCurl);
        System.out.println(Utils.TrytesToInt(address, 1) + ":" + address);
        hot.mayExists(address);
        hot.print();

        // shutdown and rebuild
        System.out.println("ShutDown Rebuild");
        hot.ShutDown();

        hot = new HotBF();
        hot.ini(1, 4 * 1024 * 8, 2, 0.001, 4 * 1024 * 8 * 5 * 3);

        hot.print();
        hot.ShutDown();

    }

    public static void mutithread() {
        // clear();
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        int num = 4;
        ArrayList<micromonitor> threads=new ArrayList<>();
        for (int i = 0; i < num; i++) {
            micromonitor m = hot.newMicroMonitor();
            threads.add(m);
            m.start();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            threads.forEach(t->{
                t.exit=true;
            });
            hot.ShutDown();
            hot.reporter.report();
            GroupBloomFilter.reporter.report();
            Buffer.reporter.report();
        }, "Shutdown Hook"));

    }

    public static void warmup() {
        HotBF hot = new HotBF();
        hot.ini(3, 4 * 1024 * 8, 2, 0.001, 100 * 1024 * 1024 * 8);
        long start = System.currentTimeMillis();
        hot.WarmUp();
        System.out.println("Warm UP time " + (System.currentTimeMillis() - start));
        hot.ShutDown();
    }

}