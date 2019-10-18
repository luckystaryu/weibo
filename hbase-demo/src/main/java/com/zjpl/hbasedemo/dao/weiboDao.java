package com.zjpl.hbasedemo.dao;

import com.zjpl.hbasedemo.constants.Constant;
import com.zjpl.hbasedemo.utils.HBaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
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

    /**
     * 发布微博
     * @param uid
     * @param content
     * @throws IOException
     */
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

        //获取用户关系
        Result result = HBaseUtils.getInstance().getDataByRowKey(Constant.USER_RELATION, uid);
        //创建一个集合，第二步操作微博收件箱
        for (Cell cell : result.rawCells()) {
            if(Constant.USER_RELATION_CF2.equals(Bytes.toString(CellUtil.cloneFamily(cell)))
                    && StringUtils.isNotBlank(Bytes.toString(CellUtil.cloneValue(cell)))){
                HBaseUtils.getInstance().putData(Constant.INBOX,Bytes.toString(CellUtil.cloneValue(cell)),Constant.INBOX_CF,CONTENT_COLUMN,content);
            }
        }
    }
    public static void attendsUser(String uid,String attend_uid) throws IOException {
        //用户uid 关注 attend_uid
        Result dataByRowKey = HBaseUtils.getInstance().getDataByRowKey(Constant.USER_RELATION, uid);
        for (Cell cell : dataByRowKey.rawCells()) {
            if (!attend_uid.equals(Bytes.toString(CellUtil.cloneValue(cell)))
                    && attend_uid.equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                HBaseUtils.getInstance().putData(Constant.USER_RELATION, uid, Constant.USER_RELATION_CF1, attend_uid, attend_uid);
                HBaseUtils.getInstance().putData(Constant.USER_RELATION, attend_uid, Constant.USER_RELATION_CF2, uid, uid);
                // }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        List<String> list = new ArrayList<>();
        list.add(Constant.CONTENT_CF);
        HBaseUtils.getInstance().createTable(Constant.WEIBO_CONTENT,list,Constant.MAXVERSION_CONTENT,false);
        List<String>  user_relationList= new ArrayList<>();
        user_relationList.add(Constant.USER_RELATION_CF1);
        user_relationList.add(Constant.USER_RELATION_CF2);
        HBaseUtils.getInstance().createTable(Constant.USER_RELATION,user_relationList,Constant.MAXVERSION_USER_RELATION,false);
        List<String> inboxList = new ArrayList<>();
        inboxList.add(Constant.INBOX_CF);
        HBaseUtils.getInstance().createTable(Constant.INBOX,inboxList,Constant.MAXVERSION_INBOX,false);
        //publisWeiBo("zhangsan","我又发微博了！");
        //List<Result> listResult= HBaseUtils.getInstance().getAllData(Constant.WEIBO_CONTENT);
        List<Result> listResult = HBaseUtils.getInstance().getPrefixRow(Constant.WEIBO_CONTENT, "zhangsan", 0);
        for (Result result : listResult) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell))+
                        "column:"+ Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "value:"+ Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        attendsUser("lisi","zhangsan");
        List<Result> userListResult = HBaseUtils.getInstance().getPrefixRow(Constant.USER_RELATION, "lisi", 0);
        for (Result userResult : userListResult) {
            for (Cell userCell : userResult.rawCells()) {
                System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(userCell))+
                        "column:"+ Bytes.toString(CellUtil.cloneQualifier(userCell))+
                        "value:"+ Bytes.toString(CellUtil.cloneValue(userCell)));
            }
        }
        List<Result> inboxResult = HBaseUtils.getInstance().getStartEndRow(Constant.WEIBO_CONTENT, "zhangsan","zhangsan|", 0);
        for (Result inboxresult : inboxResult) {
            for (Cell inboxcell : inboxresult.rawCells()) {
                System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(inboxcell))+
                        "column:"+ Bytes.toString(CellUtil.cloneQualifier(inboxcell))+
                        "value:"+ Bytes.toString(CellUtil.cloneValue(inboxcell)));
            }
        }
    }
}
