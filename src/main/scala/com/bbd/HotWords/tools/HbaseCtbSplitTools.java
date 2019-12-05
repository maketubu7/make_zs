package com.bbd.HotWords.tools;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HbaseCtbSplitTools {
    public static void main(String[] args) {
        InputStream inputStream = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String beforeStr="create 'bbd:dc_pro', {NAME => 'f1'}"
                +",SPLITS=>[";
        try {
            inputStream = PropertiesTool.class.getResourceAsStream("/hbase-split.txt");
            isr = new InputStreamReader(inputStream,"UTF-8");
            br = new BufferedReader(isr);
            String line=null;
            StringBuffer buffer = new StringBuffer("");
            while((line = br.readLine())!=null){
                buffer.append("'"+line+"',");
            }
            String finalStr = buffer.toString();
            finalStr = finalStr.substring(0,finalStr.length()-1);
            System.out.println(beforeStr+finalStr+"]");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try{br.close();isr.close();inputStream.close();}catch (Exception e){}

        }
    }
}
