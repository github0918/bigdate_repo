package com.qianfeng.service.impl;

import com.qianfeng.bean.RawSQLResult;
import com.qianfeng.repostory.ClickHouseQuery;
import com.qianfeng.service.UserLabelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.List;
import java.util.Map;

/**
 * @Description: 标签查询服务
 * @Author: QF
 * @Date: 2020/7/30 9:59 AM
 * @Version V1.0
 */

@Service
public class UserLabelServiceImpl implements UserLabelService {

    @Autowired
    private ClickHouseQuery clickHouseQuery;


    /**
     *
     * @param andList  与操作的标签集合  eg: [["gender","女"],["email_suffix","139.net"]]]
     * @param orList    或操作的标签集合 eg: [["model","小米4"]]
     * @param op 两个集合直接的操作关系  [AND  OR]  eg: "and"
     * @return    自动生成SQL，查询ClickHouse数据
     */
    @Override
    public RawSQLResult getUsersByLabel(List<List> andList, List<List> orList, String op) {
        //使用clickhouse的工具类进行查询
        List<Map<String, Object>> rawResult = clickHouseQuery.queryUsersByLabel(andList,orList,op);
        //初始化输出对象
        RawSQLResult rsr = new RawSQLResult();
        if (rawResult == null) {
            rsr.setCode(-1);
            rsr.setMsg("clickhouse query error,please check your sql");
        } else {
            rsr.setMsg("ok");
            rsr.setCode(0);
        }
        rsr.setData(rawResult);
        return rsr;
    }

    /**
     *
     * @param uid 用户ID
     * @return 用户所有标签
     */
    @Override
    public RawSQLResult getLabelByUid(String uid) {
        //调用工具类中的具体实现
        List<Map<String, Object>> rawResult = clickHouseQuery.queryLabelByUid(uid);
        RawSQLResult rsr = new RawSQLResult();
        if (rawResult == null) {
            rsr.setCode(-1);
            rsr.setMsg("clickhouse query error,please check your sql");

        } else {
            rsr.setMsg("ok");
            rsr.setCode(0);
        }
        rsr.setData(rawResult);
        return rsr;
    }
}
