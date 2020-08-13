package com.embedGroup.hotBF;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.embedGroup.hotBF.GroupBloomFilter.mayExistsResponce;
import com.mongodb.Block;

import org.iota.jota.utils.SeedRandomGenerator;

public class HotBF {
    private int prefixLength;
    private int BFUsize;
    private int hashFunctions;
    private int BFunits;
    double P;
    private int limitedSize;
    private int BlockNumber;
    public int[] Scales;// scale times for Group Bloom Filters
    private int Scaled = 0;// Scaled=max(Scales)
    ConcurrentLinkedQueue<Integer> BlockLRU = new ConcurrentLinkedQueue<>();
    HashMap<Integer, GroupBloomFilter> BlockMap = new HashMap<>();

    //MetricRegistry metrics = new MetricRegistry();
    //ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.MICROSECONDS)
            //.convertDurationsTo(TimeUnit.MICROSECONDS).build();
    /**
     * 
     * @param prefixlength
     * @param bfusize
     * @param hashfunctions
     * @param p
     * @param limitedsize Bloom filters size in memory threhold(bits)
     */
    public void ini(int prefixlength, int bfusize, int hashfunctions, double p, int limitedsize) {
        prefixLength = prefixlength;
        BFUsize = bfusize;
        hashFunctions = hashfunctions;
        P = p;
        limitedSize = limitedsize;

        BlockNumber = (int) Math.pow(27, prefixLength);
        GroupBloomFilter gb = new GroupBloomFilter(BFUsize, hashFunctions, P);
        BFunits = gb.BFUnits;
        Scales = new int[BlockNumber];
        // build block map
        // At First,no Active BFU
        

        // load metadata

        File f = new File("HBF-Meta");
        if (!f.exists()) {
            // Bootstrap,Initialize HBF file
            try {
                f.createNewFile();
                File data = new File("HBF0");
                if (!data.exists()) {
                    data.createNewFile();
                    RandomAccessFile rf = new RandomAccessFile(data, "rw");
                    rf.setLength((long) gb.Size() * BlockNumber / 8);
                    rf.close();
                } else {
                    System.out.println("ERROR:HBF but without HBF-Meta");
                }

            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
            }
            //initialize GroupBloomFIlters
            for (int i = 0; i < BlockNumber; i++) {
                GroupBloomFilter g = new GroupBloomFilter(BFUsize, hashFunctions, P, this, i);
                g.setPath("HBF" + 0);
                g.iniBFULRU();
                BlockMap.put(i, g);
            }
            // initialize BlockLRU,BFULRU
            for (int i = 0; i < BlockNumber; i++) {
                BlockLRU.add(i);
                
            }

        } else {
            try {
                // Not bootstrap,Load metadata
                BufferedReader br = new BufferedReader(new FileReader(f));
                // Scales
                String[] scas = br.readLine().split(" ");
                for (int i = 0; i < BlockNumber; i++) {
                    Scales[i] = Integer.valueOf(scas[i]);
                    if (Scales[i] > Scaled)
                        Scaled = Scales[i];
                }
                // add Scaled Blocks
                for (int i = 0; i <= Scaled; i++) {
                    for (int j = 0; j < BlockNumber; j++) {
                        //Scaled Block should with a 0.25*P
                        GroupBloomFilter g = new GroupBloomFilter(BFUsize, hashFunctions, 5+i, this, j);
                        g.setPath("HBF" + i);
                        BlockMap.put(i * BlockNumber + j, g);
                    }
                }

                // BLockLRU
                String queue = br.readLine();
                String[] q = queue.split(" ");
                if (!(q.length == BlockMap.size())) {
                    System.out.println("metadata error");
                }
                for (String s : q) {
                    BlockLRU.add(Integer.valueOf(s));
                }
                // entitis | active BFUs
                // BFULRU
                for (int i = 0; i < BlockMap.size(); i++) {
                    String meta1 = br.readLine();
                    String meta2 = br.readLine();
                    BlockMap.get(i).loadmeta(meta1, meta2);
                }
                br.close();
                // load in active BFU
                for (int i = 0; i <= Scaled; i++) {
                    // Scaled blocks are persisted in new file
                    File data = new File("HBF" + i);
                    if (!data.exists()) {
                        System.out.println("ERROR:NO DATA");
                        return;
                    }
                    RandomAccessFile ra=new RandomAccessFile(data, "r");
                    byte[] tmp=new byte[BFUsize];
                    for (int k = 0; k < BlockNumber; k++) {
                        GroupBloomFilter g=BlockMap.get(i*BlockNumber+k);
                        for (int j = 0; j < g.BFUnits; j++) {
                            if (BlockMap.get(i * BlockNumber + k).isActive(j)) {
                                //BlockMap.get(i * BlockNumber + k).LoadInBFU(j); massive cost way
                                BloomFilter<String> bf=new FilterBuilder(BFUsize,hashfunctions).buildBloomFilter();

                                ra.seek((long)(k*g.Size())+j*BFUsize);
                                ra.read(tmp);
                                bf.setBloom(tmp);
                                g.Group.put(j, bf);
                            }
                        }
                    }
                    ra.close();
                }
            } catch (Exception e) {
                // TODO: handle exception
            }

        }
        //reporter.start(5, TimeUnit.SECONDS);
    }

    // save all Active BFU
    // save metaData
    public void ShutDown() {
        /*for (int i = 0; i < BlockMap.size(); i++) {
            BlockMap.get(i).ShutDown();
        }*/
        savaData();

        File meta = new File("HBF-Meta");
        try {
            if (meta.exists())
                meta.delete();

            BufferedWriter bw = new BufferedWriter(new FileWriter(meta, true));
            // Scales
            for (int i = 0; i < BlockNumber; i++) {
                bw.write(Scales[i] + " ");
            }
            bw.write("\n");

            // BlockLRU
            Iterator<Integer> it = BlockLRU.iterator();
            while (it.hasNext()) {
                bw.write(it.next() + " ");
            }
            bw.write("\n");

            for (int i = 0; i < BlockMap.size(); i++) {
                BlockMap.get(i).savemeta(bw);
            }
            bw.close();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }

    public void WarmUp(){
        //bring all of GBF's first BFU into memory
        //RAW bootstrap
        int threshold=limitedSize/BFUsize;
        if(BlockNumber < threshold)
            threshold=BlockNumber;
        
        for(int i=0;i<threshold;i++){
            String addr=Utils.IntToTrytes(i, prefixLength)+"AAAAAAA";
            mayExists(addr);
        }
    }
    public void savaData(){
        for(int i=0;i<=Scaled;i++){
            String path="HBF"+i;
            File f=new File(path);
            try {
                if(!f.exists()) {System.out.println("DATA File Not Exists");}

                RandomAccessFile rs=new RandomAccessFile(f, "w");
                for(int j=0;j<BlockNumber;j++){
                    GroupBloomFilter g=BlockMap.get(i*BlockNumber+j);
                    for(int k=0;k<g.BFUnits;k++){
                        if(g.isActive(k)){
                            rs.seek((long)(j*g.Size()+k*BFUsize));
                            rs.write(g.getBFU(k).getBloomAsByteArray());
                        }
                    }
                }
                rs.close();
            } catch (Exception e) {
                //TODO: handle exception
            }//47ms
        }
    }

    public int prefixLength() {
        return prefixLength;
    }

    public ConcurrentLinkedQueue<Integer> BlockLRU() {
        return BlockLRU;
    }

    public void Eliminated(int numbers) {
        // choose from numbers oldest Block(with BFUs>0 in memory),everyone choose
        // oldest BFU to remove
        Iterator<Integer> it = BlockLRU.iterator();
        int target = numbers;

        // gather enough activeBFUs
        int activeGBF = 0;
        int activeBFU = 0;
        while (it.hasNext()) {
            int t = it.next();
            int BFUs;
            if ((BFUs = BlockMap.get(t).Actives()) > 0) {
                activeGBF++;
                activeBFU += BFUs;
                if (activeGBF >= numbers)
                    break;
            }
        }
        it = BlockLRU.iterator();
        if (activeGBF >= numbers) {
            // everyone eliminate one BFU
            while (target > 0) {
                int t = 0;
                if (it.hasNext()) {
                    t = it.next();
                } else {
                    System.out.println("Eliminated No Target");
                }

                GroupBloomFilter gb = BlockMap.get(t);
                if (gb.isActive()) {
                    target-=gb.EliminateBFU(1);;
                }
            }
        } else if (activeBFU >= numbers) {
            // eliminate all left BFUs
            while (target > 0) {
                int t = 0;
                t = it.next();
                GroupBloomFilter gb = BlockMap.get(t);
                int left = gb.Actives();
                if (left > 0) {
                    if (left > target) {
                        left = target;
                    }
                    target -= gb.EliminateBFU(left);
                }
            }
        } else {
            System.out.println("NO ENOUGH BFU TO ELIMINATE");
        }

    }

    // Total Active BFUs in memory
    public int TotalActives() {
        int result = 0;
        for (int i = 0; i < BlockMap.size(); i++) {
            result += BlockMap.get(i).Actives();
        }
        return result;
    }
    //Timer t3=metrics.timer("Elininate");
    public void tryEliminated() {
        int T = TotalActives();
        int limitedBFU = limitedSize / BFUsize;
        if (T * BFUsize > limitedSize) {
            //Timer.Context ctx=t3.time();
            Eliminated(T - limitedBFU);
            //ctx.close();
        }
    }

    /**
     * Obtain the block number according to the prefix of the address, load all BFUs
     * related to this block into the memory and insert the address to update. If
     * the size reaches the limit, select BFU to move out of the memory according to
     * the principle of LRU
     */
    public void Insert(String address) {
        int prefix = Utils.TrytesToInt(address, prefixLength);
        int currentBlock = Scales[prefix] * BlockNumber + prefix;
        // System.out.println(currentBlock);
        GroupBloomFilter G = BlockMap.get(currentBlock);
        G.Insert(address);// load all BFUs of block into memory

        // move block to queue tail
        BlockLRU.remove(currentBlock);
        BlockLRU.add(currentBlock);

        // if hit the threshold,try remove BFUs following LRU
        tryEliminated();

        // check capacity
        synchronized (this) {
            if (G.isFull()) {
                Scales[prefix]++;
                // if this is the first block get full
                if (Scales[prefix] > Scaled)
                    Scale();

            }
        }
    }
    
       
        //Timer t2=metrics.timer("mayExists");
    /**
     * First, check the BFU of the memory one by one, and return false directly if
     * it returns negative. Otherwise, load the BFU from the disk one by one, and
     * check, until the end, if all return positive, return true
     */
    public boolean mayExists(String address) {
        int prefix = Utils.TrytesToInt(address, prefixLength);
        boolean Result = false;
        boolean TryEliminate = false;
        // check Block from newly generated to old
        for (int i = Scales[prefix]; i >= 0; i--) {
            int currentBlock = i * BlockNumber + prefix;
            //update BlockLRU
            BlockLRU.remove(currentBlock);
            BlockLRU.add(currentBlock);

            //Timer.Context ctx=t2.time();
            mayExistsResponce result = BlockMap.get(currentBlock).mayExists(address);
            //ctx.close();
            if (result.loadIn = true) {
                TryEliminate = true;// any block load in new BFU
            }
            if (result.exists == true) {
                Result = true;
                break;
            }
        }

        if (TryEliminate)
            tryEliminated();
        return Result;
    }

    // one of current GroupBloomFilter is full,since the mean distribution,it will
    // be soon all Group get full
    // create and setlength of new "HBF"file
    public void Scale() {
        Scaled++;
        File f = new File("HBF" + (Scaled));
        if (!f.exists()) {
            try {
                f.createNewFile();
                RandomAccessFile rf = new RandomAccessFile(f, "rw");
                rf.setLength((long) (Scaled + 5) * BFUsize * BlockNumber / 8);// every scale add one new BFU
                rf.close();
            } catch (Exception e) {
                // TODO: handle exception
            }
        }

        // add new group of Blocks with same prefix
        // compared with old block,new block hava one more BFU
        //double newP = P / Math.pow(4,Scaled);   // ,Scaled P<=P0/(1-r)
        for (int i = 0; i < BlockNumber; i++) {
            //GroupBloomFilter newG = new GroupBloomFilter(BFUsize, hashFunctions, newP);
            GroupBloomFilter newG=new GroupBloomFilter(BFUsize,hashFunctions,5+Scaled,this,i);
            newG.setPath("HBF" + Scaled);
            newG.iniBFULRU();
            BlockMap.put(Scaled*BlockNumber+i, newG);
            //reschedule BlockLRU
            ConcurrentLinkedQueue<Integer> newBlockLRU=new ConcurrentLinkedQueue<>();
            for(int j=0;j<BlockNumber;j++)
                newBlockLRU.add(Scaled*BlockNumber+j);
            
            Iterator<Integer> it=BlockLRU.iterator();
            while(it.hasNext())
                newBlockLRU.add(it.next());
            
            BlockLRU=newBlockLRU;
        }

    }

    public void print(){
        Iterator<Integer> it=BlockLRU.iterator();
        System.out.printf("BlockLRU:");
        while(it.hasNext()){
            System.out.printf("%d ",it.next().intValue());
        }
        System.out.printf("\n");

        for(int i=0;i<BlockMap.size();i++){
            BlockMap.get(i).print();
        }
    }

    class micromonitor extends Thread{
        int index=0;
        HotBF hot;
        micromonitor(HotBF h){
            hot=h;
        }
        public void run(){
            while(true){
                if(index%100 == 0){
                    hot.Insert(SeedRandomGenerator.generateNewSeed());
                    System.out.println(this.getId()+" index "+index);
                }
                else{
                    hot.mayExists(SeedRandomGenerator.generateNewSeed());
                }
                index++;
            }
        }
    }

    public micromonitor newMicroMonitor(){
        return new micromonitor(this);
    }
}