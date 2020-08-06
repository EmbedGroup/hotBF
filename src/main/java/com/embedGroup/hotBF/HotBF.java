package com.embedGroup.hotBF;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.mongodb.Block;

import ch.qos.logback.classic.pattern.Util;
import freemarker.cache.ConcurrentCacheStorage;

public class HotBF {
    private int prefixLength;
    private int BFUsize;
    private int hashFunctions;
    double P;
    private int limitedSize;
    private int BlockNumber;
    ConcurrentLinkedQueue<Integer> BlockLRU=new ConcurrentLinkedQueue<>();
    HashMap<Integer,GroupBloomFilter> BlockMap=new HashMap<>();

    public void ini(int prefixlength, int bfusize, int hashfunctions, double p,int limitedsize) {
        prefixLength = prefixlength;
        BFUsize = bfusize;
        hashFunctions = hashfunctions;
        P = p;
        limitedSize=limitedsize;

        BlockNumber = (int) Math.pow(27, prefixLength);
        GroupBloomFilter gb=new GroupBloomFilter(BFUsize, hashFunctions, P);

        // IF first,create new file;Else, do nothing
        try {
            File f = new File("HBF");
            if (!f.exists()) {
                RandomAccessFile rf = new RandomAccessFile(f, "rw");
                rf.setLength(gb.Size() * BlockNumber);
                rf.close();
            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        //initialize BlockLRU
        for(int i=0;i<BlockNumber;i++){
            BlockLRU.add(i);
        }
        //build block map
        for(int i=0;i<BlockNumber;i++){
            BlockMap.put(i, new GroupBloomFilter(BFUsize, hashFunctions, P,this,i));
        }

    }
    public int prefixLength(){
        return prefixLength;
    }
    public ConcurrentLinkedQueue<Integer> BlockLRU(){
        return BlockLRU;
    }
    
    public void Eliminated(int numbers){
        //choose from numbers oldest Block(with BFUs>0 in memory),everyone choose oldest BFU to remove
        Iterator<Integer> it=BlockLRU.iterator();
        int target=numbers;
        while(target>0){
            int t=0;
            if(it.hasNext()){
                t=it.next();
            }else{
                System.out.println("Eliminated No Target");
            }

            GroupBloomFilter gb=BlockMap.get(t);
            if(gb.isActive()){
                gb.EliminateBFU(1);
                target--;
            }
        }   
    }
    //Total Active BFUs in memory
    public int TotalActives(){
        int result=0;
        for(int i=0;i<BlockNumber;i++){
            result+=BlockMap.get(i).Actives();
        }
        return result;
    }

    public void tryEliminated(){
        int T=TotalActives();

        if(T*BFUsize > limitedSize){
            Eliminated((int)Math.ceil((double)T*BFUsize-limitedSize));
        }
    }
}