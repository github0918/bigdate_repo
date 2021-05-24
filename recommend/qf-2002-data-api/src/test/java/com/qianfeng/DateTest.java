package com.qianfeng;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTest {
    public static void main(String[] args) {
        Calendar cal = Calendar.getInstance();
        for (int i = 1; i <= 10; i++) {
            //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

            try {
                Date start = sdf.parse("20200914");
                cal.setTime(start);
                cal.add(Calendar.DAY_OF_YEAR, i);
                //cal.add(Calendar.DATE, i);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            String rrDay = sdf.format(cal.getTime()); //未来的第rrday天
            System.out.println("rrDay" + rrDay + "--------------------");
        }
    }
}
