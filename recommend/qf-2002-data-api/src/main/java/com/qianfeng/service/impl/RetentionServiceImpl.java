package com.qianfeng.service.impl;

import com.qianfeng.bean.RetentionCurveInfo;
import com.qianfeng.repostory.PrestoQuery;
import com.qianfeng.service.RetentionService;
import com.qianfeng.util.Leastsq;
import com.qianfeng.util.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.ujmp.core.Matrix;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Description: 拟合留存率函数
 * @Author: QF
 * @Date: 2020/6/25 12:27 PM
 * @Version V1.0
 */

@Service
public class RetentionServiceImpl implements RetentionService {

    private static final Logger logger = LoggerFactory.getLogger(RetentionServiceImpl.class);

    @Autowired
    private PrestoQuery prestoQuery;

    /**
     * 拟合留存率， 使用最小二乘法求解
     * 留存率函数是幂函数， y = a * x^b 为非线性函数
     * 我们无法直接使用最小二乘法的矩阵法求解，因此我们先将函数做变化，转换成线性函数求解，
     * 函数两边取自然对数
     * lny = lna+b*lnx
     * <p>
     * 令 f = lny
     * 令 g = lnx
     * 变换后的函数为   f = lna +b*g
     * <p>
     * 拟合变换后的线性函数，求出 lna 和 b 的值，那么原函数a的值为e^a ，b的值依然为b
     */
    @Override
    public RetentionCurveInfo curveFit(String startDate, String endDate, int gap, int scale) {
        // 查询留存率获取原始样本数据  dwb_news.usr
        List<Sample> samples = prestoQuery.queryRetentionRate(startDate, endDate, gap);
        if (samples.size() == 0) {
            logger.warn("presto query rsu table no data");
            return null;
        }

        // 变换样本空间，因为我们对原函数取了对数，因此样本空间也要取对数
        List<Sample> convertSamples = new ArrayList<>();
        for (Sample sample : samples) {
            Sample convertSample = new Sample();
            convertSample.setX(Math.log(sample.getX()));
            convertSample.setY(Math.log(sample.getY()));
            convertSamples.add(convertSample);
        }


        // 最小二乘法求解
        Matrix theta = Leastsq.matrixSolve(convertSamples);
        if (theta == null) {
            logger.warn("leastsq calc error");
            return null;
        }

        // 求出的参数结果也是一个矩阵，他是一个 2 * 1 的矩阵，每一行代表一个求出参数值
        // 变换后的线性函数 lna 的值 ，0,0 第一行第一列的值
        double theta0 = theta.getAsDouble(0, 0);
        // 变换后的线性函数 b  的值， 1，0 第二行第一列的值
        double theta1 = theta.getAsDouble(1, 0);


        // 这里的theta0是变换后的函数a的值。 原函数 a 的值应该是 e^theta0
        Double a = Math.pow(Math.E, theta0);
        //  这里的theta1是变换后的函数b的值，也是原函数b的值，不理解看变换的公式
        Double b = theta1;


        //保留a的小数点3位，向上取整
        BigDecimal bda = new BigDecimal(a);
        a = bda.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

        BigDecimal bdb = new BigDecimal(b);
        b = bdb.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();

        //构建返回对象--->封装留存相关信息
        RetentionCurveInfo rci = new RetentionCurveInfo();
        rci.setTheta0(a);
        rci.setTheta1(b);
        rci.setEquation(String.format("y = %s * X^%s", a, b));  //封装表达式
        rci.setSamples(samples);  //封装样本数据集
        //封装未来多少天的留存率
        rci.setRrMap(calcRR(a, b, startDate, scale));
        return rci;
    }


    // 根据计算出的曲线，计算指定日期内的留存率
    private LinkedHashMap<String, Double> calcRR(Double a, Double b, String startDate, int scale) {
        Calendar cal = Calendar.getInstance();
        LinkedHashMap<String, Double> rrMap = new LinkedHashMap<>();
        for (int i = 1; i <= scale; i++) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            //SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

            try {
                Date start = sdf.parse(startDate);
                cal.setTime(start);
                cal.add(Calendar.DAY_OF_YEAR, i);   //从起始日期往后＋i天
            } catch (ParseException e) {
                logger.error("calc rr format date error", e);
            }

            String rrDay = sdf.format(cal.getTime()); //未来的第rrday天
            //我们的拟合好的函数  0.875*100^-0.653
            Double rr = a * Math.pow(i, b);  //第i日的留存率

            //取3位小数，向上取整
            BigDecimal bdrr = new BigDecimal(rr);
            rr = bdrr.setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue();
            //存储rrDay天的留存率
            rrMap.put(rrDay, rr);
        }
        return rrMap;
    }
}
