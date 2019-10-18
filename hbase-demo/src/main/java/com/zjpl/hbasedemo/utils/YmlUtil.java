package com.zjpl.hbasedemo.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.util.Properties;

public class YmlUtil {
    private final Logger logger = LoggerFactory.getLogger(YmlUtil.class);
    private static Properties prop =new Properties();
    public YmlUtil() {
        //1.加载配置文件
        Resource app = new ClassPathResource("application.yml");
        Resource appDev = new ClassPathResource("application-dev.yml");
        Resource appProd = new ClassPathResource("application-prod.yml");
        YamlPropertiesFactoryBean yamlPropertiesFactoryBean = new YamlPropertiesFactoryBean();
        //2:将加载的配置文件交给 yamlPropertiesFactoryBean
        yamlPropertiesFactoryBean.setResources(app);
        //3.将yaml转换成key,val
        Properties properties = yamlPropertiesFactoryBean.getObject();
        String active = properties.getProperty("spring.profiles.active");
        if(active =="" || active ==null){
            // logger.error("未找到spring.profile.active配置");
        }else{
            //判断当前配置是什么环境
            if("dev".equals(active)){
                yamlPropertiesFactoryBean.setResources(app,appDev);

            }else if ("prod".equals(active)){
                yamlPropertiesFactoryBean.setResources(app,appProd);
            }
        }
        prop = yamlPropertiesFactoryBean.getObject();
    }

    public static String getStrYmlValue(String keyword){
        String value =prop.getProperty(keyword);
        return value;
    }
    public static Integer getIntYmlValue(String keyword){
        String value =prop.getProperty(keyword);
        return Integer.parseInt(value);
    }
}
