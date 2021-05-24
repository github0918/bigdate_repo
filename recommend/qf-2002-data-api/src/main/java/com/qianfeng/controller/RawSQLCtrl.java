package com.qianfeng.controller;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.qianfeng.bean.RawSQLResult;
import com.qianfeng.service.RawSQLService;
import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

/**
 * @Description: 通过SQL查询Presto数据
 * @Author: QF
 * @Date: 2020/6/26 10:32 PM
 * @Version V1.0
 */
@RestController
@SpringBootApplication
@RequestMapping(value = "api/v1/")
public class RawSQLCtrl {

    @Autowired
    private RawSQLService rss;

    /**
     * @param jsonParams {"sql":"select 1","base64":1}  base64字段有两个值【0，1】，1表示是经过base64编码,0表示未编码
     * @return
     */
    @RequestMapping(value = "sql", method = {RequestMethod.POST}, produces = "application/json;charset=UTF-8")
    @ResponseBody
    public RawSQLResult getResultBySQL(@RequestBody JSONObject jsonParams) {
        String sql = JSONPath.eval(jsonParams, "$.sql").toString();
        int base64 = Integer.parseInt(JSONPath.eval(jsonParams, "$.base64").toString());
        if (base64 == 1) {
            byte[] sqlBytes = Base64.decodeBase64(sql);
            sql = new String(sqlBytes);
        }
        return rss.getDataBySQL(sql);
    }
}
