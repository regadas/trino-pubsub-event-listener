package dev.regadas.trino.pubsub.listener.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

public class MBeanRegister {

    // Same as
    // https://github.com/trinodb/trino/blob/f680380ae1adbb3c47ee6953873197a0c82dc308/lib/trino-plugin-toolkit/src/main/java/io/trino/plugin/base/jmx/ObjectNameGeneratorConfig.java#L27
    private static final String CONFIG_KEY = "jmx.base-name";

    // Same as
    // https://github.com/trinodb/trino/blob/f680380ae1adbb3c47ee6953873197a0c82dc308/core/trino-main/src/main/java/io/trino/server/JmxNamingConfig.java#L20
    private static final String DEFAULT_DOMAIN_NAME = "trino";

    public static void registerMBean(Map<String, String> config, PubSubInfoMBean info) {
        try {
            var jmxDomainBase = config.getOrDefault(CONFIG_KEY, DEFAULT_DOMAIN_NAME);
            var platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            var objectName = new ObjectName(jmxDomainBase + ".listener:name=PubSubEventListener");
            platformMBeanServer.registerMBean(info, objectName);
        } catch (MalformedObjectNameException
                | NotCompliantMBeanException
                | InstanceAlreadyExistsException
                | MBeanRegistrationException e) {
            throw new RuntimeException("Failed to register MBean for PubSubEventListener", e);
        }
    }
}
