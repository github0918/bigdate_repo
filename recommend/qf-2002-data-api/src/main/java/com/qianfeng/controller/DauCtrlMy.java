package com.qianfeng.controller;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.qianfeng.bean.DauPredictInfo;
import com.qianfeng.bean.DauPredictInfoMy;
import com.qianfeng.bean.RetentionCurveInfo;
import com.qianfeng.service.RetentionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
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
@RequestMapping(value = "api/v1/my/")    //该控制器的根路径  localhost:8080/api1/v1
public class DauCtrlMy {

//alter table tbl_name rename[to|as] new_tbl_name
    @Autowired     //默认按照类型注解   @Qualifier("")   //按照名称注解
    private RetentionService retentionService;

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
    public DauPredictInfoMy predictDAU(
            @PathVariable String startDate,
            @PathVariable String endDate,
            @PathVariable int gap,
            @PathVariable int scale,
            @PathVariable int dnu) {

        DauPredictInfoMy dpi = new DauPredictInfoMy();

        // 获取拟合函数计算的留存率信息
        RetentionCurveInfo rci = retentionService.curveFit(startDate, endDate, gap, scale);
        if (rci == null || rci.getRrMap().size() == 0) {
            dpi.setCode(-1);
            dpi.setMsg("sorry , some errors");
            return dpi;
        }

        // 存放预测的每天DAU结果数据
        LinkedHashMap<String, ArrayList<String>> dauPredict = new LinkedHashMap<String, ArrayList<String>>();
        Double tmp = 0d;
        ArrayList<String> categorys = new ArrayList<>();
        ArrayList<String> daus = new ArrayList<>();
        //循环预测的指定天留存率
        for (Map.Entry<String, Double> e : rci.getRrMap().entrySet()) {
            String day = e.getKey();  //dau中的day是从留存率中获取
            Double value = e.getValue();
            tmp = value * dnu + tmp;
            //将days和dau添加到对应的数组中
            categorys.add(day);
            daus.add((dnu + tmp.intValue())+"");
        }
        //将两个数组放到map中
        dauPredict.put("categorys", categorys);
        dauPredict.put("daus", daus);
        dpi.setCode(0);
        dpi.setMsg("ok");
        dpi.setCategoryDAU(dauPredict);
        return dpi;
    }
}
