package com.qianfeng;

import com.qianfeng.util.Leastsq;
import com.qianfeng.util.Sample;
import org.junit.jupiter.api.Test;
import org.ujmp.core.Matrix;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description:
 * @Author: QF
 * @Date: 2020/6/24 11:55 PM
 * @Version V1.0
 */
public class LeastsqTests {

    /**
     * y = a * x^b 非线性函数
     * lny = lna+b*lnx 两边求对数转为线性
     * f = lny
     * g = lnx
     * f = lna +b*g
     */
    @Test
    void matrixSolveTest() {
        //测试数据
        List<Sample> samples = new ArrayList<Sample>();
        samples.add(new Sample(Math.log(1), Math.log(0.593)));
        samples.add(new Sample(Math.log(2), Math.log(0.500)));
        samples.add(new Sample(Math.log(3), Math.log(0.391)));
        samples.add(new Sample(Math.log(4), Math.log(0.348)));

        //自己测试数据
        /*List<Sample> samples = new ArrayList<Sample>();
        samples.add(new Sample(Math.log(1), Math.log(0.800)));
        samples.add(new Sample(Math.log(2), Math.log(0.660)));
        samples.add(new Sample(Math.log(3), Math.log(0.430)));
        samples.add(new Sample(Math.log(4), Math.log(0.320)));*/

        //调用最小二乘解法
        Matrix theta = Leastsq.matrixSolve(samples);
        //取第1行第1列的值，，theta0
        double theta0 = theta.getAsDouble(0, 0);
        //取第2行第1列的值，，theta1
        double theta1 = theta.getAsDouble(1, 0);
        System.out.println("theta0: "+theta0+"\t"+"theta1: "+theta1);

//        System.out.println(Math.pow(Math.E, theta0));
//        System.out.println(theta1);

        System.out.println(String.format("a is %f", Math.pow(Math.E, theta0))); //a=e^b
        System.out.println(String.format("b is %f", theta1));
    }
}
