package dev.regadas.trino.pubsub.listener.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
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
    private static final String MBEAN_OBJECT_NAME_FORMAT =
            "%s.listener.PubSubEventListener:name=EventPublish,eventType=%s,status=%s";

    public static void registerMBean(Map<String, String> config, CountersPerEventType counters) {
        try {
            var jmxDomainBase = config.getOrDefault(CONFIG_KEY, DEFAULT_DOMAIN_NAME);
            var platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
            registerCounters(jmxDomainBase, platformMBeanServer, counters.queryCompleted());
            registerCounters(jmxDomainBase, platformMBeanServer, counters.queryCreated());
            registerCounters(jmxDomainBase, platformMBeanServer, counters.splitCompleted());

        } catch (MalformedObjectNameException
                | NotCompliantMBeanException
                | InstanceAlreadyExistsException
                | MBeanRegistrationException e) {
            throw new RuntimeException("Failed to register MBean for PubSubEventListener", e);
        }
    }

    private static void registerCounters(
            String jmxDomainBase, MBeanServer platformMBeanServer, EventCounters counters)
            throws MalformedObjectNameException,
                    InstanceAlreadyExistsException,
                    MBeanRegistrationException,
                    NotCompliantMBeanException {
        var eventType = counters.eventType();
        registerCounter(jmxDomainBase, platformMBeanServer, eventType, counters.succeeded());
        registerCounter(jmxDomainBase, platformMBeanServer, eventType, counters.failed());
    }

    private static void registerCounter(
            String jmxDomainBase,
            MBeanServer platformMBeanServer,
            String eventType,
            Counter counter)
            throws MalformedObjectNameException,
                    InstanceAlreadyExistsException,
                    MBeanRegistrationException,
                    NotCompliantMBeanException {
        var objectName = builObjectName(jmxDomainBase, eventType, counter);
        platformMBeanServer.registerMBean(counter, objectName);
    }

    private static ObjectName builObjectName(
            String jmxDomainBase, String eventType, Counter counter)
            throws MalformedObjectNameException {
        return new ObjectName(
                MBEAN_OBJECT_NAME_FORMAT.formatted(jmxDomainBase, eventType, counter.kind()));
    }
}
