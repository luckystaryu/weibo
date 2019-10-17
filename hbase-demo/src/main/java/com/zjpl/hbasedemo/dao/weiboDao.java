package com.zjpl.hbasedemo.dao;

import com.zjpl.hbasedemo.constants.Constant;
import com.zjpl.hbasedemo.utils.HBaseUtils;
import org.apache.hadoop.hbase.client.Result;
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
    public static void publisWeiBo(String uid, String content){
        String CONTENT_COLUMN ="content";
        try {
            HBaseUtils.getInstance().putData(Constant.WEIBO_CONTENT,uid,Constant.COLUMN_FAMILY_CONTENT,CONTENT_COLUMN,content);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        List<String> list = new ArrayList<>();
        list.add(Constant.COLUMN_FAMILY_CONTENT);
        HBaseUtils.getInstance().createTable(Constant.WEIBO_CONTENT,list,Constant.MAXVERSION_CONTENT,false);
        //publisWeiBo("zhangsan","我发微博了！");
        List<Result> listResult= HBaseUtils.getInstance().getAllData(Constant.WEIBO_CONTENT);
        for (Result result : listResult) {
        }
    }
}
