package com.qianfeng.service;

import com.qianfeng.bean.RawSQLResult;

import java.util.List;


/**
 * @Description:  从clickhouse 查询用户标签查询
 * @Author: QF
 * @Date: 2020/6/25 12:09 PM
 * @Version V1.0
 */
public interface UserLabelService {
    // 人群圈定
    public RawSQLResult getUsersByLabel(List<List> andList, List<List> orList, String op);

    //获取用户指定标签
    public RawSQLResult getLabelByUid(String uid);
}
