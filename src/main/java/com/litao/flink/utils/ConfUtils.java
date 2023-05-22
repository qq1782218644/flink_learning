package com.litao.flink.utils;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

public class ConfUtils {
    private static final ParameterTool pro;

    static {
        try {
            InputStream in = ConfUtils.class.getClassLoader().getResourceAsStream("conf.properties");
            pro = ParameterTool.fromPropertiesFile(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static ParameterTool getConf() {
        return pro;
    }

}
