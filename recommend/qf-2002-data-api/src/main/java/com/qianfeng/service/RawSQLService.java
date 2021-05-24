package com.qianfeng.service;


import com.qianfeng.bean.RawSQLResult;

/**
 * @Description: 根据SQL查询Pesto数据
 * @Author: QF
 * @Date: 2020/6/25 12:09 PM
 * @Version V1.0
 */
public interface RawSQLService {

    public RawSQLResult getDataBySQL(String sql);
}
