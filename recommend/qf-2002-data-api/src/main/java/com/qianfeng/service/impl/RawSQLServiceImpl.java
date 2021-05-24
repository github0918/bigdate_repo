package com.qianfeng.service.impl;

import com.qianfeng.bean.RawSQLResult;
import com.qianfeng.repostory.PrestoQuery;
import com.qianfeng.service.RawSQLService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @Description: 根据SQL查询Pesto数据
 * @Author: QF
 * @Date: 2020/6/26 10:41 PM
 * @Version V1.0
 */

@Service
public class RawSQLServiceImpl implements RawSQLService {

    //注解一个presto的查询工具类
    @Autowired
    private PrestoQuery prestoQuery;


    //根据sql获取查询数据
    @Override
    public RawSQLResult getDataBySQL(String sql) {
        //调用prestoQuery中的queryBySQL    sql="select 1"
        List<Map<String, Object>> rawResult = prestoQuery.queryBySQL(sql);
        //定义一个RawSQLResult
        RawSQLResult rsr = new RawSQLResult();  //自己编写最终结果封装对象
        if (rawResult == null) {
            rsr.setCode(-1);
            rsr.setMsg("presto query error,please check your sql");

        } else {
            rsr.setMsg("ok");
            rsr.setCode(0);
        }
        rsr.setData(rawResult);
        return rsr;
    }
}
