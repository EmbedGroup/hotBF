package com.embedGroup.hotBF;

public class skewdata {
    public String[] data;
    private int amount;
    private double skewness;
    public Zipf dist;

    public skewdata(int size, double skew) {
        amount = size;
        skewness = skew;

        data = Utils.GenerateAddre(size);
        dist = new Zipf(amount, skew);
    }

    public String out() {
        double r = Math.random();
        for (int i = 0; i < amount; i++) {
            if (r <= dist.cumulative[i]) {
                return data[i];
            }
        }
        return data[amount - 1];
    }

}