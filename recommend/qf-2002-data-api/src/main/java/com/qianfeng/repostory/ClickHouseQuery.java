package com.qianfeng.repostory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;


/**
 * @Description: ClickHouse数据源查询
 * @Author: QF
 * @Date: 2020/6/25 1:01 PM
 * @Version V1.0
 */
@Repository
public class ClickHouseQuery {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseQuery.class);

    @Autowired
    @Qualifier("clickHouseTemplate")
    private JdbcTemplate clickHouseTemplate;

    /**
     *
     * @param andList  与操作的标签集合  eg: [["gender","女"],["email_suffix","139.net"]]]
     * @param orList    或操作的标签集合 eg: [["model","小米4"]]
     * @param op 两个集合直接的操作关系  [AND  OR]  eg: "and"
     * @return    自动生成SQL，查询ClickHouse数据
     */
    public List<Map<String, Object>> queryUsersByLabel(List<List> andList, List<List> orList, String op) {
        //生产clickhouse的sql
        String sql = generateLabelSQL(andList, orList, op);
        try {
            return clickHouseTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.error("clickhouse query user by label  sql error", e);
            return null;
        }

    }

    public List<Map<String, Object>> queryLabelByUid(String uid) {
        String sql = String.format("select ln,lv from app_news.user_profile_bitmap where bitmapContains(uv,toUInt32(%s))=1 group by ln,lv;",uid);
        try {
            return clickHouseTemplate.queryForList(sql);
        } catch (Exception e) {
            logger.error("clickhouse query label by uid sql error", e);
            return null;
        }

    }


    /**
     *
     * @param andList 与操作的标签集合
     * @param orList  或操作的标签集合
     * @param op      两个集合直接的操作关系  [AND  OR]
     * @return  生成的SQL, 这个看不明白没关系，就是将条件拼接成SQL
     */
    private String generateLabelSQL(List<List> andList, List<List> orList, String op) {

        String bitMapOpTmp = "bu1";
        String subQueryTmp = "";
        int aliasCount = 0;

        //组装and条件操作
        for (int i = 0; i < andList.size(); i++) {
            aliasCount = aliasCount + 1;
            String currentBitOP = "bu" + String.valueOf(aliasCount);
            //[["gender","女"],["email_suffix":"139.net"]]
            String ln = (String) andList.get(i).get(0); //gender  //get(i) == ["gender","女"]
            String lv = (String) andList.get(i).get(1); //女
            String currentSubQueryTmp = generateSingleSubQuery(ln, lv, String.valueOf(aliasCount));
            if (andList.size() == 1) {
                subQueryTmp = currentSubQueryTmp;
                bitMapOpTmp = currentBitOP;
                break;
            }

            //and有两个及以上的条件
            if (aliasCount >= 2) {
                subQueryTmp = genJoinQuery(subQueryTmp, currentSubQueryTmp, String.valueOf(aliasCount));
            } else {
                subQueryTmp = currentSubQueryTmp;
            }

            String unionSubQuery = subQueryTmp;

            String unionBitOP = genBitMapOP(bitMapOpTmp, currentBitOP, "and");
            subQueryTmp = unionSubQuery;
            bitMapOpTmp = unionBitOP;
        }

        //组装or的条件操作
        String bitMapOpOrTmp = "bu" + String.valueOf(aliasCount + 1);
        for (int i = 0; i < orList.size(); i++) {
            String currentBitOP = "bu" + String.valueOf(aliasCount + 1);
            aliasCount = aliasCount + 1;
            String ln = (String) orList.get(i).get(0);
            String lv = (String) orList.get(i).get(1);
            String currentSubQueryTmp = generateSingleSubQuery(ln, lv, String.valueOf(aliasCount));
            if (orList.size() == 1) {
                if (aliasCount >= 2) {
                    subQueryTmp = genJoinQuery(subQueryTmp, currentSubQueryTmp, String.valueOf(aliasCount));
                } else {
                    subQueryTmp = currentSubQueryTmp;
                }

                bitMapOpOrTmp = currentBitOP;
                break;
            }
            String unionSubQuery = genJoinQuery(subQueryTmp, currentSubQueryTmp, String.valueOf(aliasCount));

            String unionBitOP = genBitMapOP(bitMapOpOrTmp, currentBitOP, "or");
            subQueryTmp = unionSubQuery;
            bitMapOpOrTmp = unionBitOP;
        }

//
//            System.out.println(subQueryTmp);
//            System.out.println(bitMapOpTmp);
//            System.out.println(bitMapOpOrTmp);

        if (andList.size() == 0) {
            bitMapOpTmp = "";
        }
        if (orList.size() == 0) {
            bitMapOpOrTmp = "";
        }

        String resBitOP = genBitMapOP(bitMapOpTmp, bitMapOpOrTmp, op);
//        System.out.println(resBitOP);
        String query = String.format("select arrayJoin(bitmapToArray(%s))as uid from %s %s", resBitOP, "\n", subQueryTmp);
        logger.warn("query clickhouse sql: %s", query);
//   System.out.println(query);
        return query;


    }

    private String genBitMapOP(String bit1, String bit2, String op) {
        String res = "";
        if ("".equals(bit1)) {
            return bit2;
        }
        if ("".equals(bit2)) {
            return bit1;
        }
        if ("and".equalsIgnoreCase(op)) {
            res = String.format("bitmapAnd(%s,%s)", bit1, bit2);
        } else {
            res = String.format("bitmapOr(%s,%s)", bit1, bit2);
        }
        return res;

    }

    private String genJoinQuery(String subQuery, String currentsubQueryTmp, String aliasNum) {

        return String.format(subQuery + "\n" + "inner join %s on t1.jid=t%s.jid", currentsubQueryTmp, aliasNum);

    }

    private String generateSingleSubQuery(String ln, String lv, String aliasNum) {
        //格式化sql语句
        return String.format("(select 1 as jid, groupBitmapMergeState(uv) as bu%s  from app_news.user_profile_bitmap" +
                " where ln='%s'  and lv='%s' ) as t%s ", aliasNum, ln, lv, aliasNum);
    }

}
