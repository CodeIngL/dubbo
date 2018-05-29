package com.alibaba.dubbo.common.utils;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p>税友软件集团有限公司</p>
 *
 * @author laihj
 *         2018/5/29
 */
public class MapUtils {

    private MapUtils() {
    }

    public static boolean isEmpty(Map map) {
        return (map == null || map.size() == 0);
    }

    public static boolean isNotEmpty(Map map) {
        return !isEmpty(map);
    }

}
