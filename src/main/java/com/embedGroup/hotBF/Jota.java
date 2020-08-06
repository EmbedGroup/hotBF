package com.embedGroup.hotBF;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bouncycastle.crypto.prng.RandomGenerator;
import org.bouncycastle.util.Times;
import org.iota.jota.IotaAPI;
import org.iota.jota.dto.response.FindTransactionResponse;
import org.iota.jota.dto.response.GetBalancesAndFormatResponse;
import org.iota.jota.dto.response.GetInclusionStateResponse;
import org.iota.jota.dto.response.SendTransferResponse;
import org.iota.jota.dto.response.WereAddressesSpentFromResponse;
import org.iota.jota.model.Input;
import org.iota.jota.model.Transaction;
import org.iota.jota.model.Transfer;
import org.iota.jota.utils.IotaAPIUtils;
import org.iota.jota.utils.SeedRandomGenerator;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Jota {
    public IotaAPI api;
    public String seed0 = "SEED99999999999999999999999999999999999999999999999999999999999999999999999999999";
    public String seed1 = "HMXZHGKGHYKSFHYCLEP9ZUAEDQQ9SJBHGYCEOAUHZJCABUKKIV9VHKLQDPUHCQ99PGZXRWFK9JJJSLGVD";
    public String seed2 = "SEED9999999999999999999999999999999999999999999999999999999999999999999999999999A";
    public int security = 1;
    public int depth = 3;
    public int minWeightMagnitude = 9;

    Jota() {
        api = new IotaAPI.Builder().protocol("http").host("127.0.0.1").port(14265).build();

    }

    public void startListener() {
        listener = new ZmqLstener("127.0.0.1", "5556");
        listener.start();
    }

    public void PrintInput(String seed) {
        try {
            GetBalancesAndFormatResponse rsp = api.getInputs(seed, 1, 0, 0, 0);
            List<Input> inputs = new ArrayList<>(rsp.getInputs());

            for (int i = 0; i < inputs.size(); i++) {
                System.out.println(inputs.get(i).getAddress() + ":" + inputs.get(i).getBalance());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void printBalance(String seed) {
        int index = 0;
        while (true) {
            String address = IotaAPIUtils.newAddress(seed, 1, index, true, api.getCurl());
            FindTransactionResponse tx = api.findTransactionsByAddresses(address);
            if (tx.getHashes().length == 0 && index != 0) {
                break;
            }
            long balance = api.getBalance(100, address);

            if (balance > 0) {
                System.out.println(address + ":" + balance);
            }
            index++;
        }
    }

    public String getAddress(String seed, int index, boolean checksum) {
        return IotaAPIUtils.newAddress(seed, security, index, checksum, api.getCurl());

    }

    public void outPutAddress(String seed, int start, int amount) {

        File f = new File(seed.substring(0, 4) + start + "-" + (start + amount - 1));
        try {
            if (!f.exists())
                f.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
            for (int i = 0; i < amount; i++) {
                String addr = IotaAPIUtils.newAddress(seed, security, start + i, false, api.getCurl());
                bw.write(addr + "\n");
            }
            bw.close();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public SendTransferResponse sendMessage(String seed, String reciveAddress, String message) {
        List<Transfer> transfers = new ArrayList<>();
        transfers.add(new Transfer(reciveAddress, 0, message, "TESTTAG"));
        SendTransferResponse res = api.sendTransfer(seed, security, depth, minWeightMagnitude, transfers, null, null,
                false, false, null);
        return res;
    }

    public SendTransferResponse sendValue(String seed, String reciveAddress, int amount) {

        List<Transfer> transfers = new ArrayList<>();
        transfers.add(new Transfer(reciveAddress, amount, "TESTMESSAGE", "TESTTAG"));

        // 默认的addreminder会产生第一个未被使用过的地址，包括本次交易中的转出地址，这种情况下remainder将无法被转出
        // 稍微修改generateNewAddress

        SendTransferResponse res = api.sendTransfer(seed, security, depth, minWeightMagnitude, transfers, null, null,
                false, true, null);

        /*
         * List<Transaction> txs = res.getTransactions(); for (int i = 0; i <
         * txs.size(); i++) { System.out.println(txs.get(i)); }
         */
        return res;
    }

    class ZmqLstener extends Thread {
        private String host;
        private String port;
        HashMap<String, Integer> txs;

        ZmqLstener(String host, String port) {
            this.host = host;
            this.port = port;
            txs = new HashMap<>();
        }

        @Override
        public void run() {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket = context.socket(ZMQ.SUB);
            String address = "tcp://" + host + ":" + port;
            socket.connect(address);
            socket.subscribe("tx");
            socket.subscribe("sn");
            while (true) {
                byte[] reply = socket.recv(0);
                String[] data = (new String(reply).split(" "));

                if (data[0].equals("tx")) {
                    txs.put(data[1], 1);
                    // System.out.println("NEW TRANSACTION" + "\n" + "Transaction hash: " + data[1]
                    // + "\n" + "Address: "+ data[2] + "\n" + "Value: " + data[3] + "\n" + "Tag: " +
                    // data[4] + "\n");
                }
                if (data[0].equals("sn")) {
                    txs.put(data[2], 2);
                    // System.out.println("CONFIRMED" + "\n" + "Transaction hash: " + data[2] + "\n"
                    // + "Address: " + data[3] + "\n");
                }
            }
        }
    }

    ZmqLstener listener;

    public void zmqtest() {
        listener = new ZmqLstener("zmq.devnet.iota.org", "5556");
        listener.start();
        while (true) {

        }
    }

    // closely related to mileston
    public void confirmLatencyCheck() {
        listener = new ZmqLstener("127.0.0.1", "5556");
        listener.start();
        int times = 0;
        long totalLatency = 0;

        try {
            while (times < 10) {
                long start = System.currentTimeMillis();
                SendTransferResponse res = sendValue(seed0, getAddress(seed1, 1000, true), 1);
                Transaction tailTx = res.getTransactions().get(1);
                while (true) {
                    if (listener.txs.get(tailTx.getHash()) == 2) {
                        break;
                    }
                    Thread.sleep(1000L);
                }
                long latency = System.currentTimeMillis() - start;
                System.out.println(latency);
                totalLatency += latency;
                times++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Average:" + ((double) totalLatency / times));
        }

    }

    public void DataMarkplaceSimulation() {
        String consumer = seed0;
        String providerA = SeedRandomGenerator.generateNewSeed();
        String providerB = SeedRandomGenerator.generateNewSeed();
        String providerC = SeedRandomGenerator.generateNewSeed();

        String dataReciver = getAddress(consumer, 100000, true);
        String valueReciverA = getAddress(providerA, 0, true);
        String valueReciverB = getAddress(providerB, 0, true);
        String valueReciverC = getAddress(providerC, 0, true);

        int amount = 10;

        // listener = new ZmqLstener("127.0.0.1", "5556");
        // listener.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            api.AddressLocalityOutput();
        }, "Shutdown Hook"));

        int times = 0;

        try {
            while (true) {
                times++;
                if (times == 10) {
                    File f = new File("addressLocality");
                    try {
                        if (!f.exists())
                            f.createNewFile();
                        BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
                        api.AddressLocalityWrite(bw);
                        bw.close();
                    } catch (Exception e) {
                        // TODO: handle exception
                    }
                }
                long start = System.currentTimeMillis();
                SendTransferResponse res = new SendTransferResponse();
                if (times % 3 == 0) {
                    sendMessage(providerA, dataReciver, "DATAFROMA");
                    res = sendValue(consumer, valueReciverA, amount);
                } else if (times % 3 == 1) {
                    sendMessage(providerB, dataReciver, "DATAFROMB");
                    res = sendValue(consumer, valueReciverB, amount);
                } else if (times % 3 == 2) {
                    sendMessage(providerC, dataReciver, "DATAFROMC");
                    res = sendValue(consumer, valueReciverC, amount);
                }
                Transaction tailTx = res.getTransactions().get(1);
                while (true) {
                    if (listener.txs.get(tailTx.getHash()) == 2) {
                        break;
                    }
                    Thread.sleep(1000L);
                }

                System.out.println("run " + times + ":" + (System.currentTimeMillis() - start) + "ms");
            }
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    public void GenerateSeed(int number) {
        File f = new File("SEED" + number);
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            for (int i = 0; i < number; i++) {
                String address = SeedRandomGenerator.generateNewSeed();
                bw.write(address + "\n");
            }
            bw.close();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public void GenerateSnapshot(int SeedNumber, int amount) {
        File snapshot = new File("snapshot.txt");
        File seeds = new File("SEED1000");
        long remain = 2779530283277761L;
        try {
            if (!seeds.exists()) {
                System.out.println("file not exists");
                return;
            }
            if (snapshot.exists())
                snapshot.delete();
            snapshot.createNewFile();
            BufferedReader br = new BufferedReader(new FileReader(seeds));
            BufferedWriter bw = new BufferedWriter(new FileWriter(snapshot, false));
            for (int i = 0; i < SeedNumber; i++) {
                String seed = br.readLine();
                String address = IotaAPIUtils.newAddress(seed, security, 0, false, api.getCurl());
                bw.write(address + ";" + amount + "\n");
                remain -= amount;
            }
            bw.write(SeedRandomGenerator.generateNewSeed() + ";" + remain + "\n");
            bw.close();
            br.close();
            System.out.println("generate finished");
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }

    public void addressUsage(String seed) {
        int i = 0;
        while (true) {
            String address = IotaAPIUtils.newAddress(seed, 1, i, true, api.getCurl());
            FindTransactionResponse tx = api.findTransactionsByAddresses(address);
            if (tx.getHashes().length == 0 && i != 0) {
                break;
            }
            long balance = api.getBalance(100, address);
            System.out.printf(balance + " ");
            WereAddressesSpentFromResponse wasSpent = api.wereAddressesSpentFrom(address);
            if (wasSpent.getStates()[0] == true) {
                System.out.printf("spent\n");
            } else {
                System.out.printf("unspent\n");
            }

            i++;
        }
    }

}