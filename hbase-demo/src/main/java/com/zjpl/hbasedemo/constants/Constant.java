package com.zjpl.hbasedemo.constants;

/**
 * USER YXQ
 * DATE 2019/10/17 15:39
 * Description
 */
public class Constant {

    /**
     * 微博内容表
     */
    public static final String WEIBO_CONTENT="ZJPL:weibo_content";
    public static final String CONTENT_CF = "info";
    public static final Integer MAXVERSION_CONTENT= 1;

    /**
     * 用户关系表
     */
    public static final String USER_RELATION="ZJPL:user_relation";
    public static final String USER_RELATION_CF1 ="attends";
    public static final String USER_RELATION_CF2 ="fans";
    public static final Integer MAXVERSION_USER_RELATION= 1;

    /**
     * 收件箱
     */
    public static final String INBOX ="ZJPL:inbox";
    public static final String INBOX_CF ="info";
    public static final Integer MAXVERSION_INBOX= 2;
}
