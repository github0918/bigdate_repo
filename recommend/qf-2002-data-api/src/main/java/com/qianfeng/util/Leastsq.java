package com.qianfeng.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.ujmp.core.DenseMatrix;
import org.ujmp.core.Matrix;

import java.util.List;

/**
 * @Description: 矩阵法求解线性最小二乘，当前只支持 y=a+bx的形式，不支持多项式
 * @Author: QF
 * @Date: 2020/6/24 9:44 PM
 * @Version V1.0
 */
public class Leastsq {

    private static Logger logger = LoggerFactory.getLogger(Leastsq.class);


    /**
     * 矩阵解法的公式 𝜃= (𝐗^𝐓 * 𝐗)^-1 * 𝐗^T*𝐘 及 X的转置乘以X求逆矩阵之后再乘以X的转置再乘以Y
     *
     * @param samples 样本数据
     * @return theta
     **/
    public static Matrix matrixSolve(List<Sample> samples) {
        try {
            //定义样本数据中中列
            int coefCount = 2;
            // 初始化一个矩阵 samples.size * coefCount ,存储样本特征数据, 因为我们支持 y=a+bx ，因此这个矩阵是一个
            // samples.size * 2 的矩阵 ，矩阵第一列是常数项1 第二列是会赋值为 x 的值，就是样本特征值
            Matrix matrixX = DenseMatrix.Factory.ones(samples.size(), coefCount);
            // 赋值矩阵第二列X的值
            for (int i = 0; i < samples.size(); i++) {
                // (i,1)表示第 i 行，第2列 ，
                matrixX.setAsDouble(samples.get(i).getX(), i, 1);
            }

            // 初始化 samples.size * 1 的矩阵，存储样本标签值，即y的值
            Matrix matrixY = DenseMatrix.Factory.ones(samples.size(), 1);
            // 把y的值赋值给矩阵
            for (int i = 0; i < samples.size(); i++) {
                matrixY.setAsDouble(samples.get(i).getY(), i, 0);
            }

            //求解公式𝜃= (𝐗^𝐓 * 𝐗)^-1 * 𝐗^T*𝐘
            // 求X的转置
            Matrix matrixXTrans = matrixX.transpose();
            // X^T*X X转置乘以X
            Matrix matrixMtimes = matrixXTrans.mtimes(matrixX);
            //  (X^T*X)^-1  X转置乘以X的逆矩阵
            Matrix matrixMtimesInv = matrixMtimes.inv();
            //  (X^T*X)^-1 * X^T   X转置乘以X的逆矩阵再乘X的转置
            Matrix matrixMtimesInvMtimes = matrixMtimesInv.mtimes(matrixXTrans);
            //  (X^T*X)^-1 * X^T * Y X转置乘以X的逆矩阵再乘X的转置再乘Y  得到最终我们要求的参数矩阵 𝜃
            return matrixMtimesInvMtimes.mtimes(matrixY);

        } catch (Exception e) {
            logger.error("leastsq error, samples", samples, e);
            return null;
        }
    }
}
