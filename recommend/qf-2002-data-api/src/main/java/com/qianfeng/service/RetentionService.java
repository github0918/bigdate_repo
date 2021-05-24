package com.qianfeng.service;


import com.qianfeng.bean.RetentionCurveInfo;

/**
 * @Description: 拟合留存率曲线
 * @Author: QF
 * @Date: 2020/6/25 12:09 PM
 * @Version V1.0
 */
public interface RetentionService {
    //预测留存信息()
    public RetentionCurveInfo curveFit(String startDate, String endDate, int gap, int scale);
}
