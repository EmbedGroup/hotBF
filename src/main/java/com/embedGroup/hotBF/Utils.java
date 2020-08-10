package com.embedGroup.hotBF;

import org.iota.jota.types.Trytes;

public class Utils {
    
    public static String IntToTrytes(int x,int length){
        if(x > Math.pow(27, length)){
            System.out.println("index outbound");
            return null;
        }
        String Trytes="";
        for(int i=0;i<length;i++){
            int k=x%27;
            x=x/27;
            if(k==0){
                Trytes='9'+Trytes;
            }
            else{
                Trytes=(char)('A'+k-1)+Trytes;
            }
        }
        return Trytes;
    }

    public static int TrytesToInt(String addr,int length){
        int result=0;
        for(int i=0;i<length;i++){
            char k=addr.charAt(i);
            if(k=='9'){
                result+=0;
            }else{
                result+=(int)(k-'A')+1;
            }
            if(i!=length-1)   result*=27;
        }
        return result;
    }
}