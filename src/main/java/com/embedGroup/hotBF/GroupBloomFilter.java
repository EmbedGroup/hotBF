package com.embedGroup.hotBF;

import java.io.BufferedWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * a GroupBloomFilter is consists of a set of independent BlommFilterUnit.
 * BFUsize:The size of a BFU,so entrie size of GBF N=BFUsize*BFUnits. P:the
 * false positive of GBF.P=p^BFUnits,p=false positive of BFU;
 * hashFunction:number of hash functions for one BFU,total hash
 * functions=hashFunction*BFUnits=K=log2(1/P)
 */
public class GroupBloomFilter {
    private int index;
    //public CopyOnWriteArrayList<BloomFilter<String>> Group =new CopyOnWriteArrayList<>();//Thread safety for write and delete operations
    public ConcurrentHashMap<Integer,BloomFilter<String>> Group=new ConcurrentHashMap<>();
    private int BFUsize;
    private int HashFunctions;
    private double P;
    private String path;
    public int BFUnits;// BFunits=log2(1/P) / hashFUnctions

    private int Size;// M=BFUsize*BFUnits
    private int Capacity;// M*(ln2)^2 / |lnP|
    private int Entities;//current number of enttities inseted to group bloom filter 
    HotBF hot;

    private boolean[] Active;//true if BFU in memory
    private int Actives;//BFU numbers in memory


    //Deque
    ConcurrentLinkedDeque<Integer> BFULRU=new ConcurrentLinkedDeque<>();//BFULRU was born empty
    GroupBloomFilter(int bfusize, int hashFunctions, double p,HotBF hotbf,int Index) {
        BFUsize = bfusize;
        HashFunctions = hashFunctions;
        P = p;
        hot=hotbf;
        index=Index;

        int K = (int) (Math.ceil(Math.log(1 / P) / Math.log(2)));
        
        BFUnits = K / hashFunctions;
        Size = BFUsize * BFUnits;
        double e = 2.7182818285;
        Capacity = (int) (Size * Math.pow((Math.log(2) / Math.log(e)),2) / Math.abs((Math.log(P) / Math.log(e))) );

        Active=new boolean[BFUnits];
        for(int i=0;i<BFUnits;i++)  {
            Active[i]=false;
            //BFULRU.add(i);
        }
        Actives=0;
        
    }
    GroupBloomFilter(int bfusize, int hashFunctions, double p) {
        BFUsize = bfusize;
        HashFunctions = hashFunctions;
        P = p;

        int K = (int) (Math.ceil(Math.log(1 / P) / Math.log(2)));
        
        BFUnits = K / hashFunctions;
        Size = BFUsize * BFUnits;
        double e = 2.7182818285;
        Capacity = (int) (Size * Math.pow((Math.log(2) / Math.log(e)),2) / Math.abs((Math.log(P) / Math.log(e))) );

        Active=new boolean[BFUnits];
        for(int i=0;i<BFUnits;i++)  Active[i]=false;

        
    }
    public int Size(){
        return Size;
    }
    public void iniBFULRU(){
        for(int i=0;i<BFUnits;i++){
            BFULRU.add(i);
        }
    }
    public void setPath(String p){
        path=p;
    }
    public void loadmeta(String meta1,String meta2){
        //meta1,entites | active
        String[] m1=meta1.split(" ");
        Entities=Integer.valueOf(m1[0]);
        for(int i=1;i<m1.length;i++){
            Active[Integer.valueOf(m1[i])]=true;
            Actives++;
        }
        //meta2 BFULRU
        String[] m2=meta2.split(" ");
        for(int i=0;i<m2.length;i++){
            BFULRU.add(Integer.valueOf(m2[i]));
        }
        
    }
    public void savemeta(BufferedWriter bw){
        try {
            bw.write(String.valueOf(Entities)+" ");
            for(int i=0;i<BFUnits;i++){
                if(Active[i]) bw.write(String.valueOf(i)+" ");
            }
            bw.write("\n");
            Iterator<Integer> it=BFULRU.iterator();
            while(it.hasNext()){
                bw.write(String.valueOf(it.next())+" ");
            }
            bw.write("\n");
        } catch (Exception e) {
            //TODO: handle exception
        }
        
    }
    // hash value,output range[1,m],k hash results
    public static int[] hashCassandra(byte[] value, int m, int k) {
        int[] result = new int[k];
        long hash1 = HashProvider.murmur3(0, value);
        long hash2 = HashProvider.murmur3((int) hash1, value);
        for (int i = 0; i < k; i++) {
            result[i] = (int) ((hash1 + i * hash2) % m);
        }
        return result;
    }

    public int[] getHashValues(String s) {
        return hashCassandra(s.getBytes(), BFUsize, BFUnits * HashFunctions);
    }

    public void add(String s) {
        int[] hashedValues = getHashValues(s);

        for (int i = 0; i < BFUnits; i++) {
            BloomFilter<String> BFU = Group.get(i);
            for (int j = 0; j < HashFunctions; j++) {
                BFU.setBit(hashedValues[i * HashFunctions + j], true);
            }
        }
    }

    public boolean check(String s) {
        int[] hashedValues = getHashValues(s);
        for (int i = 0; i < BFUnits; i++) {
            BloomFilter<String> BFU = Group.get(i);
            for (int j = 0; j < HashFunctions; j++) {
                if (!BFU.getBit(hashedValues[i * HashFunctions + j])) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkBFU(String s, int i) {
        if (i >= BFUnits) {
            System.out.println("ERROR:check unexit BFU");
            System.exit(1);
        }
        int[] hashedValues = getHashValues(s);
        BloomFilter<String> BFU = Group.get(i);
        for (int j = 0; j < HashFunctions; j++) {
            if (!BFU.getBit(hashedValues[i * HashFunctions + j])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @param BFUs         specific number of one BF unit
     * @param hashedValues compelete hashed value for a key
     * @return
     */
    public boolean checkBFUWithHashValues(int BFUs, int[] hashedValues) {
        BloomFilter<String> BFU = Group.get(BFUs);

        for (int i = 0; i < HashFunctions; i++) {
            if (!BFU.getBit(hashedValues[BFUs * HashFunctions + i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * insert address to GroupBF,this will bring all BFUs into memory(if necessary)
     * @param address
     */
    public void Insert(String address){
        //load all BFU into memory
        for(int i=0;i<BFUnits;i++){
            if(!Active[i]){
                //load BFU into memory
                BloomFilter<String> bf=new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
                bf.load(path, getOffset(index, i), BFUsize);
                Group.put(i, bf);
                Active[i]=true;
            }
        }
        Entities++;
        Actives=BFUnits;
        //note that,this time we dont't update BFULRU,cause they don't change BFU hotness

        //update BFUs
        add(address);

    }

    public boolean isFull(){
        return Entities>=Capacity;
    }

    public class mayExistsResponce{
        boolean exists;
        boolean loadIn;
        mayExistsResponce(boolean r,boolean l){
            exists=r;loadIn=l;
        }
    }
    /**
     * First check BFUs in memory,then if needed,load in BFU to check
     * When a negative result is obtained, move the corresponding BFU to the MRU end, otherwise no change is made
     */
    public mayExistsResponce mayExists(String address){
        
        int[] keys=getHashValues(address);

        //check BFUs in memory
        for(int i=0;i<BFUnits;i++){
            if(Active[i]){
                boolean result=checkBFUWithHashValues(i, keys);
                if(result==false){
                    
                    //update BFULRU
                    //Since Deque, ew can view the tail of the linked list to avoid unnecessary operations
                    if(BFULRU.peekLast()!=i){
                        BFULRU.remove(i);
                        BFULRU.add(i);
                    }
                    //get result;
                    return (new mayExistsResponce(false, false));
                }       
            }
        }
        //else,need to bring other BFUs into memory
        if(Actives==BFUnits) return (new mayExistsResponce(true, false));
        
        for(int i=0;i<BFUnits;i++){
            if(!Active[i]){
                //read into memory
                BloomFilter<String> bf=new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
                bf.load(path, getOffset(index, i), BFUsize);
                Group.put(i, bf);
                Active[i]=true;
                Actives++;

                //check
                boolean result=true;
                for (int j = 0; j < HashFunctions; j++) {
                    if (!bf.getBit(keys[i * HashFunctions + j])) {
                        result=false;
                        break;
                    }
                }

                if(result==false){
                    //The one who contributed
                    BFULRU.remove(i);
                    BFULRU.add(i);
                    return (new mayExistsResponce(false, true));
                }
                
            }
        }
        
        return (new mayExistsResponce(true, true));
    }

    public long getOffset(int block,int Units){
        return (long)block*Size+Units*BFUsize;
    }

    public int Actives(){
        return Actives;
    }
    public boolean isActive(){
        return Actives>0;
    }
    public boolean isActive(int index){
        return Active[index];
    }

    //save and remove the LRU BFU
    public boolean EliminateBFU(int numbers){
        if(Actives < numbers){
            System.out.println("No Enough BFU to Eliminate");
            return false;
        }
        Iterator<Integer> it=BFULRU.iterator();
        int target=numbers;
        while(target>0){
            int t;
            t=it.next();
            if(Active[t]){
                Group.get(t).save(path, getOffset(index, t), BFUsize);//save before remove
                Group.remove(t);//remove BFU
                Active[t]=false;
                Actives--;
                target--;
            }
        }
        return true;
    }

    public void ShutDown(){
        //save active BFU
        for(int i=0;i<BFUnits;i++){
            if(Active[i]){
                Group.get(i).save(path, getOffset(index, i), BFUsize);
            }
        }
    }

    public void LoadInBFU(int k){
        if(!Active[k]){
            System.out.println("Load in non-active BFU");
            return;
        }
        BloomFilter<String> bf=new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
        bf.load(path, getOffset(index, k), BFUsize);
        Group.put(k, bf);
    }
}