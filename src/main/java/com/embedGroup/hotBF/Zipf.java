package com.embedGroup.hotBF;

import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class Zipf {

    private static final double Constant = 1.0;
    private NavigableMap<Double, Integer> map;
    int size;
    double skewness;
    double[] frequency;
    double[] cumulative;
    public Zipf(int R, double F) {
        // create the TreeMap
        frequency=new double[R];
        cumulative=new double[R];
        size=R;
        skewness=F;
        map = computeMap(R, F);
        rank();
        
    }

    // size为rank个数，skew为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高
    private static NavigableMap<Double, Integer> computeMap(int size, double skew) {
        NavigableMap<Double, Integer> map = new TreeMap<Double, Integer>();
        // 总频率
        double div = 0;
        // 对每个rank，计算对应的词频，计算总词频
        for (int i = 1; i <= size; i++) {
            // the frequency in position i
            div += (Constant / Math.pow(i, skew));
        }
        // 计算每个rank对应的y值，所以靠前rank的y值区间远比后面rank的y值区间大
        double sum = 0;
        for (int i = 1; i <= size; i++) {
            double p = (Constant / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, i - 1);
        }
        return map;
    }

    
    public void rank(){
        NavigableMap.Entry<Double, Integer> lastentry=map.firstEntry();
        int i=0;
        for (NavigableMap.Entry<Double, Integer> entry : map.entrySet()){
            if(entry.getValue()==0) frequency[0]=entry.getKey();
            else{
                int index=entry.getValue();
                frequency[index]=entry.getKey()-lastentry.getKey();
                lastentry=entry;
            }

            cumulative[i++]=entry.getKey();
        }
    }
    public void test(){
        for (NavigableMap.Entry<Double, Integer> entry : map.entrySet()) {
            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
    }
    public void print(){
        for(int i=0;i<size;i++){
            System.out.println(cumulative[i]);
        }
    }
    public void print(int index){
        for(int i=0;i<index;i++){
            System.out.println(frequency[i]);
        }
    }
}