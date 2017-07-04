package com.alibaba.dubbo.common.uitl;

import com.alibaba.dubbo.common.utils.ConfigUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p>税友软件集团有限公司</p>
 *
 * @author laihj
 * @date 2017/6/30
 * @see
 */
public class ConfigUtilTest {

    @Test
    public void testReplaceProperty() {
        Map<String, String> map = new HashMap<>();
        map.putIfAbsent("a", "1");
        map.putIfAbsent("bb", "3333");
        map.putIfAbsent("c", "a");
        String expression = "${${c}}${bb}${c}";
        String result = ConfigUtils.replaceProperty(expression, map);
        System.out.println(result);
    }
}
