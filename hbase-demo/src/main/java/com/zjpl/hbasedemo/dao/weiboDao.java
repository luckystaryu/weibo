package com.zjpl.hbasedemo.dao;

import com.zjpl.hbasedemo.constants.Constant;
import com.zjpl.hbasedemo.utils.HBaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * USER YXQ
 * DATE 2019/10/17 14:52
 * Description
 * 1.发布微博
 * 2.关注用户
 * 3.取关用户
 */
public class weiboDao {
    public static void publisWeiBo(String uid, String content) throws IOException {
        //获取时间戳
        long ts = System.currentTimeMillis();
        //获取RowKey
        String rowKey = uid+"_"+ts;
        String CONTENT_COLUMN ="content";
        try {
            HBaseUtils.getInstance().putData(Constant.WEIBO_CONTENT,rowKey,Constant.CONTENT_CF,CONTENT_COLUMN,content);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //创建一个集合，用于存放微博内容表的对象

        //第二步操作微博收件箱
        Result result = HBaseUtils.getInstance().getDataByRowKey(Constant.USER_RELATION, uid);
        for (Cell columnCell : result.rawCells()) {

        }
    }
    public static void main(String[] args) throws IOException {
        List<String> list = new ArrayList<>();
        list.add(Constant.CONTENT_CF);
        HBaseUtils.getInstance().createTable(Constant.WEIBO_CONTENT,list,Constant.MAXVERSION_CONTENT,false);
        //publisWeiBo("zhangsan","我发微博了！");
        List<Result> listResult= HBaseUtils.getInstance().getAllData(Constant.WEIBO_CONTENT);
        for (Result result : listResult) {
            for (Cell cell : result.rawCells()) {
                System.out.println(CellUtil.cloneRow(cell));
            }
        }
    }
}
