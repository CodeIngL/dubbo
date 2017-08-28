package com.alibaba.dubbo.rpc.support;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;

/**
 * @author <a href="mailto:gang.lvg@alibaba-inc.com">kimi</a>
 */
public class ProtocolUtils {

    private ProtocolUtils() {
    }

    /**
     * 返回服务标识（serviceGroup+"/"+serviceName+":"serviceVersion+":"port）
     * @param url 元信息
     * @return key 服务标识
     * @see #serviceKey(int, String, String, String)
     */
    public static String serviceKey(URL url) {
        return serviceKey(url.getPort(), url.getPath(), url.getParameter(Constants.VERSION_KEY),
                          url.getParameter(Constants.GROUP_KEY));
    }

    /**
     * 返回服务标识（serviceGroup+"/"+serviceName+":"serviceVersion+":"port）
     * @param port 端口
     * @param serviceName 服务全类名
     * @param serviceVersion 版本
     * @param serviceGroup 分类
     * @return serviceGroup+"/"+serviceName+":"serviceVersion+":"port
     */
    public static String serviceKey(int port, String serviceName, String serviceVersion, String serviceGroup) {
        StringBuilder buf = new StringBuilder();
        if (serviceGroup != null && serviceGroup.length() > 0) {
            buf.append(serviceGroup);
            buf.append("/");
        }
        buf.append(serviceName);
        if (serviceVersion != null && serviceVersion.length() > 0 && !"0.0.0".equals(serviceVersion)) {
            buf.append(":");
            buf.append(serviceVersion);
        }
        buf.append(":");
        buf.append(port);
        return buf.toString();
    }

    /**
     * 是否是泛化调用
     * @param generic 泛化属性
     * @return 是否泛化调用
     */
    public static boolean isGeneric(String generic) {
        return generic != null && !"".equals(generic) && (Constants.GENERIC_SERIALIZATION_DEFAULT.equalsIgnoreCase(generic)  /* 正常的泛化调用 */
            || Constants.GENERIC_SERIALIZATION_NATIVE_JAVA.equalsIgnoreCase(generic) /* 支持java序列化的流式泛化调用 */
            || Constants.GENERIC_SERIALIZATION_BEAN.equalsIgnoreCase(generic));
    }

    public static boolean isDefaultGenericSerialization(String generic) {
        return isGeneric(generic)
            && Constants.GENERIC_SERIALIZATION_DEFAULT.equalsIgnoreCase(generic);
    }

    public static boolean isJavaGenericSerialization(String generic) {
        return isGeneric(generic)
            && Constants.GENERIC_SERIALIZATION_NATIVE_JAVA.equalsIgnoreCase(generic);
    }

    public static boolean isBeanGenericSerialization(String generic) {
        return isGeneric(generic) && Constants.GENERIC_SERIALIZATION_BEAN.equals(generic);
    }
}
