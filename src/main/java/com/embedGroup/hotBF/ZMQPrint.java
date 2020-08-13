package com.embedGroup.hotBF;

import org.zeromq.ZMQ;

class ZMQPrint extends Thread{
    private String host;
    private int port;
    public AddressLocality lo;
    ZMQPrint(String host, int port) {
        this.host = host;
        this.port = port;
        lo=new AddressLocality();
    }
    public void run() {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.SUB);
        String address = "tcp://" + host + ":" + port;
        socket.connect(address);
        socket.subscribe("tx");
        socket.subscribe("sn");
        int index=0;
        while (true) {
            byte[] reply = socket.recv(0);
            String[] data = (new String(reply).split(" "));
            while((index++)%100==0){
                System.out.println(index+" TX recived");
            }
            if (data[0].equals("tx")) {
                
                 //System.out.println("NEW TRANSACTION" + "\n" + "Transaction hash: " + data[1]
                 //+ "\n" + "Address: "+ data[2] + "\n" + "Value: " + data[3] + "\n" + "Tag: " +
                 //data[4] + "\n");
                String addr=data[2];
                lo.put(addr);
            }
            
/*            if (data[0].equals("sn")) {
                
                 System.out.println("CONFIRMED" + "\n" + "Transaction hash: " + data[2] + "\n"
                 + "Address: " + data[3] + "\n");
            }*/
        }
    }
}