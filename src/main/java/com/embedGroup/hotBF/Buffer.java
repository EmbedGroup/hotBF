package com.embedGroup.hotBF;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class Buffer {
    // private int BufferSize;
    private int AvaliableBFU;
    public ConcurrentHashMap<Integer, BloomFilter<String>> buffer;
    private HotBF hot;
    private int scale;// every buffer holds BFU for a HBF file,so as Flush() will can settle in one
                      // file
    private int BFUsize;

    public static MetricRegistry metrics = new MetricRegistry();
    public static ConsoleReporter reporter = Utils.getreport(metrics);
    public static Timer flusht=metrics.timer("Buffer Flush");

    public Buffer(int buffersize, HotBF h, int Scale) {
        // BufferSize=buffersize;
        hot = h;
        BFUsize=hot.getBFUSize();
        
        AvaliableBFU = buffersize / BFUsize;
        buffer = new ConcurrentHashMap<>();
        scale = Scale;
        
    }

    // put BFU into buffer
    // index=GroupIndex*BFUnites+BFUIndex
    public void put(int index, BloomFilter<String> bf) {
        buffer.put(index, bf);
        AvaliableBFU--;
        if (AvaliableBFU <= 0) {
            tryFlush();
        }
    }

    public BloomFilter<String> get(int index) {
        return buffer.get(index);
    }

    public boolean contains(int index){
        return buffer.containsKey(index);
    }

    public void remove(int index){
        buffer.remove(index);
    }
    synchronized public void tryFlush() {
        if (AvaliableBFU <= 0) {
            Flush();
        }
    }

    public void Flush() {
        Timer.Context ctx=flusht.time();
        String path = "HBF" + scale;
        File f = new File(path);
        if (f.exists()) {
            try {
                RandomAccessFile rs = new RandomAccessFile(f, "rw");
                Iterator<Map.Entry<Integer, BloomFilter<String>>> entrites = buffer.entrySet().iterator();
                // reorder
                // Map<Integer,BloomFilter<String>> map=new TreeMap<>();
                Map<Integer, BloomFilter<String>> map = new TreeMap<>(buffer);
                // remove all BFU
                while (entrites.hasNext()) {
                    entrites.next();
                    // map.put(entry.getKey(), entry.getValue());
                    entrites.remove();
                    AvaliableBFU++;
                }

                // save ordered BFU
                entrites = map.entrySet().iterator();
                while (entrites.hasNext()) {
                    Map.Entry<Integer, BloomFilter<String>> entry = entrites.next();
                    rs.seek( entry.getKey() *(long)BFUsize /8);
                    rs.write(entry.getValue().getBloomAsByteArray());
                }

                rs.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("BUFFER FLUSH FILE NOT EXISTS");

        }
        ctx.close();
    }
}