package com.embedGroup.hotBF;

import java.security.acl.Group;
import java.util.ArrayList;
import java.util.BitSet;

/**
 * a GroupBloomFilter is consists of a set of independent BlommFilterUnit.
 * BFUsize:The size of a BFU,so entrie size of GBF N=BFUsize*BFUnits. P:the
 * false positive of GBF.P=p^BFUnits,p=false positive of BFU;
 * hashFunction:number of hash functions for one BFU,total hash
 * functions=hashFunction*BFUnits=K=log2(1/P)
 */
public class GroupBloomFilter {

    public ArrayList<BloomFilter<String>> Group = new ArrayList<>();
    private int BFUsize;
    private int HashFunctions;
    private double P;

    public int BFUnits;// BFunits=log2(1/P) / hashFUnctions

    private int Size;// M=BFUsize*BFUnits
    private int Capacity;// M*(ln2)^2 / |lnP|

    GroupBloomFilter(int bfusize, int hashFunctions, double p) {
        BFUsize = bfusize;
        HashFunctions = hashFunctions;
        P = p;

        int K = (int) (Math.ceil(Math.log(1 / P) / Math.log(2)));
        
        BFUnits = K / hashFunctions;
        Size = BFUsize * BFUnits;
        double e = 2.7182818285;
        Capacity = (int) (Size * Math.pow((Math.log(2) / Math.log(e)),2) / Math.abs((Math.log(P) / Math.log(e))) );

        for (int i = 0; i < BFUnits; i++) {
            BloomFilter<String> f = new FilterBuilder(BFUsize, hashFunctions).buildBloomFilter();
            Group.add(f);
        }

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

}