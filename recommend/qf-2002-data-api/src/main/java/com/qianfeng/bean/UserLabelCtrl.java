package com.qianfeng.bean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;

import com.qianfeng.service.UserLabelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @Description: 通过SQL查询Presto数据
 * @Author: QF
 * @Date: 2020/6/26 10:32 PM
 * @Version V1.0
 */
@RestController
@SpringBootApplication
@RequestMapping(value = "api/v1/")
public class UserLabelCtrl {

    @Autowired
//    @Qualifier(value = "userLabelServiceImpl")
    private UserLabelService uls;

    /**
     * 根据用户标签圈定人群 , and 集合表示标签直接的关系是与，or集合表示标签直接关系是或。 op 表示两个集合之间的关系
     * 实现自定义组合标签查询，当前只支持等值查询
     * @param jsonParams eg: {"and":[["gender","女"],["email_suffix":"139.net"]],"or":[["model","小米4"]],"op":"and"}
     * @return  用户ID列表
     */
    @RequestMapping(value = "label/users", method = {RequestMethod.GET}, produces = "application/json;charset=UTF-8")
    @ResponseBody
    public RawSQLResult getResultBySQL(@RequestBody JSONObject jsonParams) {

        //从参数中解析对应的参数值出来
       JSONArray jaAnd =  jsonParams.getJSONArray("and");
       String  and = JSONObject.toJSONString(jaAnd);
        List<List> andList =JSONObject.parseArray(and,List.class);

        JSONArray jaOr =  jsonParams.getJSONArray("or");
        String  or = JSONObject.toJSONString(jaOr);
        List<List> orList =JSONObject.parseArray(or,List.class);

        String op = JSONPath.eval(jsonParams, "$.op").toString();
        //调用服务
        return uls.getUsersByLabel(andList,orList,op);
    }


    //根据uid查询对应的所有标签
    @RequestMapping(value = "user/{uid}/label", method = {RequestMethod.GET})
    @ResponseBody
    public RawSQLResult getLabelByUser(@PathVariable String uid) {
        return uls.getLabelByUid(uid);
    }
}
