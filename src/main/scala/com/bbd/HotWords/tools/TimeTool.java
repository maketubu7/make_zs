package com.bbd.HotWords.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeTool {
    static DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static DateFormat formatDate = new SimpleDateFormat("yyyy-MM-dd");

    public static java.sql.Timestamp getSqlTimeByString(String inStr){
        java.sql.Timestamp timestamp = null;
        try{
            Date date = format.parse(inStr);
            timestamp = new java.sql.Timestamp(date.getTime());
        }catch (Exception e){

        }
        return timestamp;
    }

    public static java.sql.Date getSqlDateByString(String inStr){
        java.sql.Date date = null;
        try{
            Date date1 = formatDate.parse(inStr);
            date = new java.sql.Date(date1.getTime());
        }catch (Exception e){

        }
        return date;
    }

    public static String getRealDateStr(String inStr){
        String reStr = inStr;
        if(inStr==null || "".equals(inStr)) return inStr;
        try{
            if(inStr.indexOf("-") != -1){//正常日期格式
               //do nothing
            }else{//long格式
                Date date = new Date(Long.valueOf(inStr));
                reStr = format.format(date);
            }
        }catch (Exception e){

        }
        return reStr;
    }

    public static String getRealDateStrDate(String inStr){
        String reStr = inStr;
        if(inStr==null || "".equals(inStr)) return inStr;
        try{
            if(inStr.indexOf("-") != -1){//正常日期格式
                //do nothing
            }else{//long格式
                Date date = new Date(Long.valueOf(inStr));
                reStr = formatDate.format(date);
            }
        }catch (Exception e){

        }
        return reStr;
    }

    public static boolean isFromatStr(String inStr){
        if(inStr == null) return false;
        Boolean bol = false;
        try{
            format.parse(inStr);
            bol = true;
        }catch (Exception e){

        }
        return bol;
    }
}
