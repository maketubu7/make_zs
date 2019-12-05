package com.bbd.zhanshen.i.IndicService;

import com.bbd.zhanshen.i.IndicService.tool.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    public static void main(String[] args) throws ParseException {
//        try{
//            long days = DateUtils.getBetweenDays("2018-01-20","2018-01-20");
//            System.out.println(days);
//        }catch (Exception e){
//            e.printStackTrace();
//        }

        Date s = new Date();
        System.out.println(s.getTime());
        s.setTime(1550735877000l);
        SimpleDateFormat sim = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(sim.format(s));
        String a = "2019-03-12 00:00:00";
        System.out.println(sim.parse(a).getTime());
        String b = "2019-03-16 00:00:00";
        System.out.println(sim.parse(b).getTime());



    }
}
