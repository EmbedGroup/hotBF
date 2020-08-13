package com.embedGroup.hotBF;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class AddressLocality {
    private HashMap<String, Integer> lastAccess;
    private int globalAccess;
    private int LocalityThrehold = 10000;
    int[] accessLocality = new int[LocalityThrehold];

    AddressLocality() {
        for (int i = 0; i < LocalityThrehold; i++)
            accessLocality[i] = 0;
        globalAccess = 0;
        lastAccess = new HashMap<>();
    }

    public void put(String address) {
        globalAccess++;
        Integer lastAccessTime = lastAccess.get(address);
        if ((lastAccessTime != null) && (globalAccess - lastAccessTime) < LocalityThrehold) {
            accessLocality[globalAccess - lastAccessTime]++;
        }
        lastAccess.put(address, globalAccess);
        if ((globalAccess % 10000) == 0) {
            write();
        }
    }

    public void output() {
        System.out.println("globalAccess " + globalAccess + " wereAddressSpentFrom Access locality:");
        for (int i = 0; i < globalAccess && i < LocalityThrehold; i++) {
            if (accessLocality[i] != 0) {
                System.out.println(i + " " + accessLocality[i]);
            }
        }
    }

    public void write() {
        try {
            // System.out.println(sdf.format(date));
            File f = new File("addressLocality");
            try {
                if (!f.exists())
                    f.createNewFile();
                BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
                Date date = new Date();
                String strDateFormat = "yyyy-MM-dd HH:mm:ss";
                SimpleDateFormat sdf = new SimpleDateFormat(strDateFormat);
                bw.write(sdf.format(date) + "\n");
                
                bw.write("globalAccess " + globalAccess + " wereAddressSpentFrom Access locality:" + "\n");
                for (int i = 0; i < globalAccess && i<LocalityThrehold; i++) {
                    if (accessLocality[i] != 0) {
                        bw.write(i + " " + accessLocality[i] + "\n");
                    }
                }

                bw.close();
            } catch (Exception e) {
                // TODO: handle exception
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void summary(){
        for(int i=1;i<=LocalityThrehold;i*=10){
            int sum=0;
            for(int j=0;j<=i && j<LocalityThrehold;j++){
                sum+=accessLocality[j];
            }
            System.out.println("Locality "+i+":"+((double)sum/globalAccess*100));
        }
    }

}