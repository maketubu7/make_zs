package com.bbd.IndicService.tool;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class DateUtils {
    static DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    static DateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static Long getBetweenDays(String startDay,String endDay) throws Exception{
        long re = 0;
        if(StringUtils.isEmpty(startDay)){
            throw new Exception("startDay不能为空!!!");
        }
        try{
            Date startDate = format.parse(startDay);
            Date endDate = null;
            if(StringUtils.isEmpty(endDay)){
                endDate = new Date();
            }else{
                endDate = format.parse(endDay);
            }

            re = Math.abs((endDate.getTime() - startDate.getTime())/(1000*3600*24));
        }catch (Exception e){

        }
         return re;
    }

    public static String addOneDayAddStr(String dateStr) throws Exception{
        Date now = format1.parse(dateStr);
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.DAY_OF_YEAR,1);
        return format1.format(cal.getTime());
    }

    public static String getOneYearBeforeStr(String dateStr) throws Exception{
        Date now = format1.parse(dateStr);
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.DAY_OF_YEAR,-365);
        return format1.format(cal.getTime());
    }

    public static String getNowDateStr() throws Exception{
        return format.format(new Date())+" 00:00:00";
    }

    public static boolean isDateStr(String str) throws Exception{
        boolean re = false;
        try{format.parse(str); re=true;}catch (Exception e){}
        try{format1.parse(str); re=true;}catch (Exception e){}
        return re;
    }

    public static String getStrByDate(Date date) throws Exception{
        return format1.format(date);
    }
}
