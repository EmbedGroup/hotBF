package com.embedGroup.hotBF;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;

import org.iota.jota.IotaAPI;
import org.iota.jota.builder.AddressRequest;
import org.iota.jota.dto.response.GetBalancesAndFormatResponse;
import org.iota.jota.dto.response.SendTransferResponse;
import org.iota.jota.model.Transaction;
import org.iota.jota.model.Transfer;
import org.iota.jota.pow.ICurl;
import org.iota.jota.pow.SpongeFactory;
import org.iota.jota.utils.IotaAPIUtils;
import org.iota.jota.utils.SeedRandomGenerator;

public class MotivationTest {
    public void GenerateAddress(int start, int amount) {
        ICurl customCurl = SpongeFactory.create(SpongeFactory.Mode.KERL);

        File f = new File("Address" + start + "-" + String.valueOf(start + amount - 1));
        long usedTime = 0;
        try {
            if (!f.exists()) {
                f.createNewFile();
            }
            FileWriter fw = new FileWriter(f, true);

            String seed = "SEED99999999999999999999999999999999999999999999999999999999999999999999999999999";
            for (int i = start; i < start + amount; i++) {

                long be = System.currentTimeMillis();
                String s = IotaAPIUtils.newAddress(seed, 1, i, false, customCurl);
                usedTime += System.currentTimeMillis() - be;

                fw.write(s + "\n");
                if (i % 1000 == 0) {
                    System.out.println(i + " generated");
                }
            }
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("generate address use " + usedTime + " ms");
    }

    public void test0() {
        GroupBloomFilter g = new GroupBloomFilter(5 * 4 * 1024 * 8, 10, 5);
        g.add("hello");
        if (!g.check("hello")) {
            System.out.println("error");
        }

    }

    public void test1() {
        GroupBloomFilter g = new GroupBloomFilter(4 * 1024 * 8, 2, 0.001);
        File file = new File("Address0-11467");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String address;
            while ((address = reader.readLine()) != null) {
                g.add(address);
            }
            reader.close();
        } catch (Exception e) {
            // TODO: handle exception
        }

        int[] checkTimes = new int[g.BFUnits + 1];
        int[] BFUContribution = new int[g.BFUnits];
        int FPTimes = 0;

        file = new File("Address20000-29999");
        // file=new File("Address0-11467");
        try {
            reader = new BufferedReader(new FileReader(file));
            String address;
            while ((address = reader.readLine()) != null) {
                int[] hashedValues = g.getHashValues(address);
                int truetime = 0;

                for (int i = 0; i < g.BFUnits; i++) {
                    if (g.checkBFUWithHashValues(i, hashedValues)) {
                        truetime++;
                    } else {
                        break;
                    }
                }

                if (truetime == g.BFUnits) {
                    FPTimes++;
                }

                checkTimes[truetime]++;

                // BFU identity
                for (int i = 0; i < g.BFUnits; i++) {
                    if (!g.checkBFUWithHashValues(i, hashedValues)) {
                        BFUContribution[i]++;
                    }
                }
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        System.out.println(
                "Check 10000 unexist address for a 11468/11468 Group Bloom Filter,times return true(=checktime-1,except FP =checktime):");
        for (int i = 0; i < g.BFUnits + 1; i++) {
            System.out.println(i + " " + checkTimes[i]);
        }
        System.out.println("False Positive times:" + FPTimes);

        System.out.println("BFU identity:");
        for (int i = 0; i < g.BFUnits; i++) {
            System.out.println(BFUContribution[i]);
        }
    }

    public void test2() {
        for (int i = 0; i < 35; i++) {
            GroupBloomFilter g = new GroupBloomFilter(5 * 4 * 1024 * 8, 10, 5);
            File f = new File("Address0-34999");
            try {
                BufferedReader bf = new BufferedReader(new FileReader(f));
                String address;
                for (int j = 0; j < i * 1000; j++) {
                    address = bf.readLine();
                    g.add(address);
                }
                bf.close();
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }

            int[] checkTimes = new int[g.BFUnits + 1];

            f = new File("Address50000-59999");
            // file=new File("Address0-11467");
            try {
                BufferedReader reader = new BufferedReader(new FileReader(f));
                String address;
                while ((address = reader.readLine()) != null) {
                    int[] hashedValues = g.getHashValues(address);
                    int truetime = 0;
                    for (int j = 0; j < g.BFUnits; j++) {
                        if (g.checkBFUWithHashValues(j, hashedValues)) {
                            truetime++;
                        } else {
                            break;
                        }
                    }
                    checkTimes[truetime]++;
                }
                reader.close();
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }

            for (int j = 0; j < g.BFUnits + 1; j++) {
                // System.out.println(j + " " + checkTimes[j]);
                System.out.printf("%d\n", checkTimes[j]);
            }

        }
    }

    private static final String TEST_SEED1 = "IHDEENZYITYVYSPKAURUZAQKGVJEREFDJMYTANNXXGPZ9GJWTEOJJ9IPMXOGZNQLSNMFDSQOTZAEETUEA";
    private static final String TEST_MESSAGE = "JUSTANOTHERIOTATEST";
    private static final String TEST_TAG = "IOTAJAVASPAM999999999999999";

    public void messageSendtest() {
        IotaAPI api = new IotaAPI.Builder().protocol("http").host("127.0.0.1").port(14265).build();

        List<Transfer> transfers = new ArrayList<>();

        AddressRequest addressRequest = new AddressRequest.Builder(TEST_SEED1, 2).checksum(true).build();
        String address = api.generateNewAddresses(addressRequest).first();
        // String address=IotaAPIUtils.newAddress(TEST_SEED1, 2, 1, true,
        // api.getCurl());
        transfers.add(new Transfer(address, 0, TEST_MESSAGE, TEST_TAG));

        SendTransferResponse str = api.sendTransfer(TEST_SEED1, 2, 3, 9, transfers, null, null, false, false, null);

        System.out.println(str.getTransactions());

    }

    public class monitor extends Thread {

        private String Consumer;
        private String Provider;
        private Jota jota;
        List<String> consumers;
        ArrayList<String> providers;
        List<String> backups;

        Zipf zc;
        Zipf zp;
        int times = 0;

        monitor(Jota j, List<String> con, ArrayList<String> pro, Zipf c, Zipf p, String Con, List<String> backs) {
            jota = j;
            consumers = con;
            providers = pro;
            zc = c;
            zp = p;
            Consumer = Con;
            backups = backs;
        }

        public void run() {
            while (true) {
                if (Consumer != Getnode(consumers, zc)) {
                    try {
                        Thread.sleep(5 * 1000);
                        continue;
                    } catch (Exception e) {
                        // TODO: handle exception
                    }
                }
                GetBalancesAndFormatResponse rsp = jota.api.getInputs(Consumer, 1, 0, 0, 10);
                if (rsp.getTotalBalance() < 10) {
                    System.out.println(Consumer + "NO ENOUGH BALANCE");
                }

                Provider = Getnode(providers, zp);
                System.out.println("Got:" + Consumer + "<->" + Provider);
                String dataReciver = jota.getAddress(Consumer, 999999, true);
                System.out.println(dataReciver);
                String valueReciver = jota.getAddress(Provider, 0, true);
                for (int i = 0; i < 10; i++) {
                    jota.sendMessage(Provider, dataReciver, "DATA");
                }
                System.out.println(valueReciver);
                SendTransferResponse res = jota.sendValue(Consumer, valueReciver, 10);

                Transaction tailTx = res.getTransactions().get(1);

                long start = System.currentTimeMillis();
                while (true) {
                    // System.out.println("confirm");
                    if (jota.listener.txs.get(tailTx.getHash()) == 2) {
                        break;
                    }
                    if (System.currentTimeMillis() - start > 60 * 1000) {
                        System.out.println(" SEED LOCKED,SEED REPLACE");
                        System.out.println(Consumer);
                        if (backups.size() > 0) {
                            int index = consumers.indexOf(Consumer);
                            consumers.remove(Consumer);
                            consumers.add(index, backups.get(0));
                            Consumer = backups.get(0);
                            backups.remove(0);
                        }
                        System.out.println(Consumer);
                        break;
                    }
                    try {
                        Thread.sleep(1000L);
                    } catch (Exception e) {
                        // TODO: handle exception
                    }
                }
            }
        }
    }

    public String Getnode(List<String> seeds, Zipf z) {
        Random r = new Random();
        double d = r.nextDouble();
        for (int i = 0; i < z.size; i++) {
            if (d <= z.cumulative[i]) {
                return seeds.get(i);
            }
        }
        return seeds.get(seeds.size() - 1);
    }

    private int providerNumber = 50;
    private int consumerNuber = 1;
    private double skewness;

    public void DataMarketPlaceSimu(double skew) {

        skewness = skew;
        // int numCPUs = Runtime.getRuntime().availableProcessors();

        Jota jota = new Jota();// listener start
        jota.startListener();
        List<String> consumers = Collections.synchronizedList(new ArrayList<>());
        ArrayList<String> providers = new ArrayList<>();
        List<String> backup = Collections.synchronizedList(new ArrayList<>());

        Zipf zc = new Zipf(consumerNuber, skewness);
        Zipf zp = new Zipf(providerNumber, skewness);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            jota.api.AddressLocalityOutput();
        }, "Shutdown Hook"));

        File f = new File("SEED1000");
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String buffer;
            for (int i = 0; i < consumerNuber; i++) {
                buffer = br.readLine();
                String seed = buffer.split(";")[0];
                consumers.add(seed);

            }
            for (int i = 0; i < providerNumber; i++) {
                buffer = br.readLine();
                String seed = buffer.split(";")[0];
                providers.add(seed);
            }
            for (int i = consumerNuber + providerNumber; i < 1000 && (buffer = br.readLine()) != null; i++) {
                String seed = buffer.split(";")[0];
                backup.add(seed);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
        }
        System.out.println("inputs before ");
        for (int i = 0; i < consumerNuber; i++) {
            GetBalancesAndFormatResponse rsp = jota.api.getInputs(consumers.get(i), 1, 0, 0, 0);
            System.out.println(consumers.get(i) + ":" + rsp.getTotalBalance());
            // jota.printBalance(consumers.get(i));
        }
        System.out.println("Frequency To Choose Every Consumer:");
        for (int i = 0; i < consumerNuber; i++) {
            System.out.println(zc.frequency[i]);
        }
        System.out.println("Frequency To Choose Every Provider:");
        for (int i = 0; i < providerNumber; i++) {
            System.out.println(zp.frequency[i]);
        }

        Thread[] ts = new Thread[consumerNuber];
        for (int i = 0; i < consumerNuber; i++) {
            ts[i] = new monitor(jota, consumers, providers, zc, zp, consumers.get(i), backup);
            ts[i].start();
        }

    }

    public void DataMarketPlaceSimu2() {
        double skewness = 0.0;
        Jota jota = new Jota();// listener start
        jota.startListener();
        List<String> consumers = Collections.synchronizedList(new ArrayList<>());
        ArrayList<String> providers = new ArrayList<>();
        List<String> backup = Collections.synchronizedList(new ArrayList<>());

        Zipf zc = new Zipf(consumerNuber, skewness);
        Zipf zp = new Zipf(providerNumber, skewness);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            jota.api.AddressLocalityOutput();
        }, "Shutdown Hook"));

        File f = new File("SEED1000");
        try {
            BufferedReader br = new BufferedReader(new FileReader(f));
            String buffer;
            for (int i = 0; i < consumerNuber; i++) {
                buffer = br.readLine();
                String seed = buffer.split(";")[0];
                consumers.add(seed);

            }
            for (int i = 0; i < providerNumber; i++) {
                buffer = br.readLine();
                String seed = buffer.split(";")[0];
                providers.add(seed);
            }
            for (int i = consumerNuber + providerNumber; i < 1000 && (buffer = br.readLine()) != null; i++) {
                String seed = buffer.split(";")[0];
                backup.add(seed);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
            // TODO: handle exception
        }
        long start = System.currentTimeMillis();
        while (true) {
            if (System.currentTimeMillis() - start > 60 * 60 * 1000) {
                break;
            }
            ArrayList<SendTransferResponse> ress = new ArrayList<>();
            ArrayList<String> sendConsumers = new ArrayList<>();
            for (int i = 0; i < consumerNuber; i++) {
                Random r = new Random();
                double d = r.nextDouble();
                if (d <= zc.cumulative[i]) {
                    // got chance to send
                    String Consumer = consumers.get(i);
                    GetBalancesAndFormatResponse rsp = jota.api.getInputs(Consumer, 1, 0, 0, 10);
                    if (rsp.getTotalBalance() < 10) {
                        System.out.println(Consumer + "NO ENOUGH BALANCE");
                    }

                    String Provider = Getnode(providers, zp);
                    System.out.println("Got:" + Consumer + "<->" + Provider);
                    String dataReciver = jota.getAddress(Consumer, 0, true);
                    String valueReciver = jota.getAddress(Provider, 0, true);
                    sendConsumers.add(Consumer);
                    jota.sendMessage(Provider, dataReciver, "DATA");
                    SendTransferResponse res = jota.sendValue(Consumer, valueReciver, 10);
                    ress.add(res);
                }
            }

            // wait until confirmed
            long Startwait = System.currentTimeMillis();
            while (true) {
                if (ress.isEmpty())
                    break;
                if (System.currentTimeMillis() - Startwait > 60 * 1000) {
                    // replace unconfimed seed
                    for (int i = 0; i < sendConsumers.size(); i++) {
                        System.out.println(" SEED LOCKED,SEED REPLACE");
                        System.out.println(sendConsumers.get(i));
                        String newconsumer = "";
                        if (backup.size() > 0) {
                            newconsumer = backup.get(0);
                            int index = consumers.indexOf(sendConsumers.get(i));
                            consumers.remove(sendConsumers.get(i));
                            consumers.add(index, backup.get(0));
                            backup.remove(0);
                        }
                        System.out.println(newconsumer);
                    }
                    break;
                }
                // check confirm
                for (int i = 0; i < ress.size(); i++) {
                    Transaction tailTx = ress.get(i).getTransactions().get(1);
                    if (jota.listener.txs.get(tailTx.getHash()) == 2) {
                        ress.remove(i);
                        sendConsumers.remove(i);
                        if (ress.size() == 0)
                            break;
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                    // TODO: handle exception
                }
            }

        }

    }
}