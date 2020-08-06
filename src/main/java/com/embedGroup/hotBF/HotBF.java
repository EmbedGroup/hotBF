package com.embedGroup.hotBF;

import java.io.File;
import java.io.RandomAccessFile;

import com.mongodb.Block;

public class HotBF {
    private int prefixLength;
    private int BFUsize;
    private int hashFunctions;
    double P;

    public void ini(int prefixlength, int bfusize, int hashfunctions, double p) {
        prefixLength = prefixlength;
        BFUsize = bfusize;
        hashFunctions = hashfunctions;
        P = p;

        int BlockNumber = (int) Math.pow(27, prefixLength);
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

    }


}