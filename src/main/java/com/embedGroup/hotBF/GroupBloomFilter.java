package com.embedGroup.hotBF;

import java.security.acl.Group;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.Block;

/**
 * a GroupBloomFilter is consists of a set of independent BlommFilterUnit.
 * BFUsize:The size of a BFU,so entrie size of GBF N=BFUsize*BFUnits. P:the
 * false positive of GBF.P=p^BFUnits,p=false positive of BFU;
 * hashFunction:number of hash functions for one BFU,total hash
 * functions=hashFunction*BFUnits=K=log2(1/P)
 */
public class GroupBloomFilter {
    private int index;
    public ArrayList<BloomFilter<String>> Group = new ArrayList<>();
    private int BFUsize;
    private int HashFunctions;
    private double P;
    
    public int BFUnits;// BFunits=log2(1/P) / hashFUnctions

    private int Size;// M=BFUsize*BFUnits
    private int Capacity;// M*(ln2)^2 / |lnP|
    HotBF hot;

    boolean[] Active;//true if BFU in memory
    int Actives;//BFU numbers in memory
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
            BFULRU.add(i);
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
     * insert address to GroupBF,this will bring all BFUs into memory(if necessary),
     * Then select the corresponding number of BFU to drain the memory
     * @param address
     */
    public void Insert(String address){
        int blockNo=Utils.TrytesToInt(address, hot.prefixLength());

        //load all BFU into memory
        for(int i=0;i<BFUnits;i++){
            if(!Active[i]){
                //load BFU into memory
                BloomFilter<String> bf=new FilterBuilder(BFUsize, HashFunctions).buildBloomFilter();
                bf.load("HBF", getOffset(blockNo, i), BFUsize);
                Group.add(i, bf);
                Active[i]=true;
            }
        }
        Actives=BFUnits;
        //note that,this time we dont't update BFULRU,cause they don't change BFU hotness

        //update BFUs
        add(address);

        //update BlockLRU
        hot.BlockLRU().remove(blockNo);
        hot.BlockLRU().add(blockNo);
        
        //remove oldest
        hot.tryEliminated();

    }

    public int getOffset(int block,int Units){
        return block*Size+Units*BFUsize;
    }

    public int Actives(){
        return Actives;
    }
    public boolean isActive(){
        return Actives==0;
    }

    //save and remove the LRU BFU
    public boolean EliminateBFU(int numbers){
        if(Actives() < numbers){
            System.out.println("No Enough BFU to Eliminate");
            return false;
        }
        Iterator<Integer> it=BFULRU.iterator();
        int target=numbers;
        while(target>0){
            int t;
            t=it.next();
            if(Active[t]){
                Group.get(t).save("HBF", getOffset(index, t), BFUsize);//save before remove
                Group.remove(t);//remove BFU
                Active[t]=false;
                target--;
            }
        }
        return true;
    }
}