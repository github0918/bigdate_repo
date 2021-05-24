package com.qianfeng.repostory;

import com.qianfeng.util.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @Description: Presto数据源查询
 * @Author: QF
 * @Date: 2020/6/25 1:01 PM
 * @Version V1.0
 */
@Repository
public class PrestoQuery {
    private static final Logger logger = LoggerFactory.getLogger(PrestoQuery.class);

    @Autowired
    @Qualifier("prestoTemplate")
    private JdbcTemplate prestoTemplate;

    // 查询指定日期范围的留存率
    public List<Sample> queryRetentionRate(String startDate, String endDate, int gap) {

        String sql = "select \n" +
                "gap , \n" +
                "sum(retention_num)*1.000/sum(new_num) as rr  \n" +
                "from dwb_news.rsu \n" +
                "where biz_date between ? and ? and gap between 1 and ? \n" +
                "group by gap order by gap";

        //使用prestoTemplate查询数据
        SqlRowSet rr = prestoTemplate.queryForRowSet(sql, startDate, endDate, gap);

        //封装样本数据
        List<Sample> samples = new ArrayList<>();
        while (rr.next()) {
            Sample sample = new Sample();
            sample.setX(rr.getInt("gap"));
            sample.setY(rr.getDouble("rr"));
            samples.add(sample);
        }
        return samples;
    }


    // 查询指定日期范围的留存率
    public List<Map<String, Object>> queryBySQL(String sql) {
        try {
            return prestoTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.error("presto query by sql error", e);
            return null;
        }
    }

}
