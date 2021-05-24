package com.qianfeng.controller;

import com.qianfeng.bean.RetentionCurveInfo;
import com.qianfeng.service.RetentionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

/**
 * @Description: 计算留存拟合函数API
 * @Author: QF
 * @Date: 2020/6/24 9:38 PM
 * @Version V1.0
 */
@RestController
@SpringBootApplication
@RequestMapping(value = "api/v1/")
public class RetentionCtrl {
    private static final Logger logger = LoggerFactory.getLogger(RetentionCtrl.class);

    @Autowired
    private RetentionService retentionService;

    /**
     * startDate 查询样本数据的开始日期，就是从presto表dwb_news.rus表查询数据的开始日期
     * endDate 终止日期，同上
     * gap 要查询的最大gap，也就是多少日留存
     * scala 要展示预测多少天的结果数据
     */
    @RequestMapping(value = "rr/{startDate}/{endDate}/{gap}/{scale}", method = {RequestMethod.GET})
    @ResponseBody
    public RetentionCurveInfo getCurveInfo(
            @PathVariable String startDate,
            @PathVariable String endDate,
            @PathVariable int gap,
            @PathVariable int scale) {
        //返回预测的留存率
        return retentionService.curveFit(startDate, endDate, gap, scale);
    }
}
