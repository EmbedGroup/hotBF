package com.embedGroup.hotBF;

import org.iota.jota.utils.IotaAPIUtils;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        //Jota jota=new Jota();
        //jota.GenerateSeed(1000);
        //jota.addressUsage("BFBUUIOKVZYXHXHYOUCNWFRDSZDWLPGLNMTYAVXENSEMJOHUBUQLBFAWAYEBBVFFNTQRMPCXSNYMS9PHD");
        //jota.PrintInput("QV9BDEJQVJHRBVKZQKYIJBCQVMUZQTSKAGHF9CUXEZPVQKGMPYJSMLVGVXVQGMUINQIY9MOUABVYUGBMD");
        //jota.GenerateSnapshot(1000, 100000);
        //jota.sendValue(jota.seed1, jota.getAddress(jota.seed0,  10, true), 3012);
        //jota.DataMarkplaceSimulation();
        //jota.confirmLatencyCheck();
        //jota.zmqtest();
        //MotivationTest t=new MotivationTest();
        //t.test1();
        //t.DataMarketPlaceSimu(4.0);
        Test t=new Test();
        t.linkedqueueTest();
        //t.RandomnessTest();
        //GroupBloomFilter g=new GroupBloomFilter(4*1024*8, 2, 0.001);
        

    }
}

