package com.qianfeng.controller;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.qianfeng.bean.DauPredictInfo;
import com.qianfeng.bean.RetentionCurveInfo;
import com.qianfeng.service.RetentionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Description: DAU预测API
 * @Author: QF
 * @Date: 2020/6/25 8:09 PM
 * @Version V1.0
 */

@RestController
@SpringBootApplication
@RequestMapping(value = "api/v1/")    //该控制器的根路径  localhost:8080/api1/v1
public class DauCtrl {

//alter table tbl_name rename[to|as] new_tbl_name
    @Autowired     //默认按照类型注解   @Qualifier("")   //按照名称注解
    private RetentionService retentionService;

    /**
     * 传入JSON参数，个字段含义如下
     * {
     * "start_date": "2020-06-20",  查询样本数据的开始日期，就是从presto表dwb_news.rus表查询数据的开始日期
     * "end_date": "2020-06-28",      终止日期，同上
     *
     * "gap": 10,     要查询的最大gap，也就是多少日留存
     * "scale": 100,  要展示预测多少天的结果数据
     * "static_dnu": 2000 固定每日新增量;
     * }
     */
    //localhost:8080/api1/v1/dau
    @RequestMapping(value = "dau", method = {RequestMethod.POST}, produces = "application/json;charset=UTF-8")
    @ResponseBody
    public DauPredictInfo predictDAU(@RequestBody JSONObject jsonParams) {
        //从jsonParams中获取对应的参数
        String startDate = JSONPath.eval(jsonParams, "$.start_date").toString();
        String endDate = JSONPath.eval(jsonParams, "$.end_date").toString();
        int gap = Integer.parseInt(JSONPath.eval(jsonParams, "$.gap").toString());
        int scale = Integer.parseInt(JSONPath.eval(jsonParams, "$.scale").toString());
        int dnu = Integer.parseInt(JSONPath.eval(jsonParams, "$.static_dnu").toString());

        DauPredictInfo dpi = new DauPredictInfo();

        // 获取拟合函数计算的留存率信息
        RetentionCurveInfo rci = retentionService.curveFit(startDate, endDate, gap, scale);
        if (rci == null || rci.getRrMap().size() == 0) {
            dpi.setCode(-1);
            dpi.setMsg("sorry , some errors");
            return dpi;
        }

        // 存放预测的每天DAU结果数据
        LinkedHashMap<String, Integer> dauPredict = new LinkedHashMap<>();
        Double tmp = 0d;
        //循环每日留存率
        for (Map.Entry<String, Double> e : rci.getRrMap().entrySet()) {
            String day = e.getKey();  //day:指多少天
            Double value = e.getValue(); //留存率
            tmp = value * dnu + tmp;  //累加未来day的日活(预测)
            dauPredict.put(day, dnu + tmp.intValue());
        }
        dpi.setCode(0);  //code  0:正常   -1：获取留存率错误
        dpi.setMsg("ok");
        dpi.setPredictDAU(dauPredict);
        dpi.setRci(rci);
        return dpi;
    }


    /**
     * GET PATH Variable 方式请求，结果同上
     * startDate 查询样本数据的开始日期，就是从presto表dwb_news.rus表查询数据的开始日期
     * endDate 终止日期，同上
     * gap 要查询的最大gap，也就是多少日留存
     * scala 要展示预测多少天的结果数据
     * static_dnu 固定每日新增量
     */
    //localhost:8080/api/v1/dau/20200920/20200922/2/10/2000
    @RequestMapping(value = "dau/{startDate}/{endDate}/{gap}/{scale}/{dnu}", method = {RequestMethod.GET})
    @ResponseBody
    public DauPredictInfo predictDAU(
            @PathVariable String startDate,
            @PathVariable String endDate,
            @PathVariable int gap,
            @PathVariable int scale,
            @PathVariable int dnu) {

        DauPredictInfo dpi = new DauPredictInfo();

        // 获取拟合函数计算的留存率信息
        RetentionCurveInfo rci = retentionService.curveFit(startDate, endDate, gap, scale);
        if (rci == null || rci.getRrMap().size() == 0) {
            dpi.setCode(-1);
            dpi.setMsg("sorry , some errors");
            return dpi;
        }

        // 存放预测的每天DAU结果数据
        LinkedHashMap<String, Integer> dauPredict = new LinkedHashMap<>();
        Double tmp = 0d;
        //循环预测的指定天留存率
        for (Map.Entry<String, Double> e : rci.getRrMap().entrySet()) {
            String day = e.getKey();  //dau中的day是从留存率中获取
            Double value = e.getValue();
            tmp = value * dnu + tmp;
            dauPredict.put(day, dnu + tmp.intValue());
        }
        dpi.setCode(0);
        dpi.setMsg("ok");
        dpi.setPredictDAU(dauPredict);
        dpi.setRci(rci);
        return dpi;
    }
}
