package org.srg.smartclient.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.event.ApplicationContextEvent;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.util.Assert;
import org.srg.smartclient.*;
import org.srg.smartclient.annotations.SmartClientHandler;

import javax.persistence.Entity;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Loads and unloads {@link org.srg.smartclient.DSHandler Handlers} when the application context is created and destroyed. Each resource provided is
 * loaded as an application context with the current context as its parent, and then all the jobs from the child context
 * are registered under their bean names. A {@link org.srg.smartclient.DSDispatcher} is required.
 *
 * @see org.srg.smartclient.DSDispatcher
 */
public class AutomaticDSHandlerRegistrar implements Ordered, SmartLifecycle, ApplicationContextAware,  InitializingBean {

    private static Logger logger = LoggerFactory.getLogger(AutomaticDSHandlerRegistrar.class);

    private ApplicationContext applicationContext;
    private int order = Ordered.LOWEST_PRECEDENCE;
    private int phase = Integer.MIN_VALUE + 1000;
    private boolean autoStartup = true;

    private IDSDispatcher dsDispatcher;
    private Object lifecycleMonitor = new Object();
    private volatile boolean running = false;

    private List<Class> entitiesFound = new LinkedList<>();
    private Set<Tuple2<String, String>> classesSkipped = new HashSet<>();
    private Set<Tuple2<String, String>> classesFailed = new HashSet<>();

    public IDSDispatcher getDsDispatcher() {
        return dsDispatcher;
    }

    public void setDsDispatcher(IDSDispatcher dsDispatcher) {
        this.dsDispatcher = dsDispatcher;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.state(dsDispatcher != null, "A DSDispatcher must be provided");
    }

    /**
     * The enclosing application context, which can be used to check if {@link ApplicationContextEvent events} come
     * from the expected source.
     *
     * @param applicationContext the enclosing application context if there is one
     * @see ApplicationContextAware#setApplicationContext(ApplicationContext)
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void start() {
        synchronized (this.lifecycleMonitor) {
            if (running) {
                return;
            }

            final SmartClientProperties properties = applicationContext.getBean(SmartClientProperties.class);
//          Assert.state(properties != null, "Smart Client properties must be provided");

            try {
                dsDispatcher.loadFromResource(properties.getSharedDirectory());
            } catch ( Exception ex) {
                throw new RuntimeException("SmartClient handler loading has been failed", ex);
            }

            try {
                register((ListableBeanFactory) applicationContext.getAutowireCapableBeanFactory());
            } catch (Throwable t) {
                throw new RuntimeException("SmartClient handler registration has been failed", t);
            }
            running = true;
        }
    }

    @Override
    public void stop() {
        synchronized (this.lifecycleMonitor) {
//            jobLoader.clear();
            running = false;
        }

    }

    @Override
    public boolean isRunning() {
        synchronized (this.lifecycleMonitor) {
            return running;
        }
    }

    @Override
    public int getOrder() {
        return order;
    }

    @Override
    public boolean isAutoStartup() {
        return autoStartup;
    }

    /**
     * @param autoStartup true for auto start.
     * @see #isAutoStartup()
     */
    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public int getPhase() {
        return phase;
    }

    /**
     * @param phase the phase.
     * @see #getPhase()
     */
    public void setPhase(int phase) {
        this.phase = phase;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    private void registerJpaEntity(Class<?> entityClass) throws Exception {
        final JpaDSDispatcher jpaDSDispatcher = getJpaDSDispatcher();
        jpaDSDispatcher.registerJPAEntity(entityClass );
    }

    protected void register(ListableBeanFactory beanFactory)  throws Exception {
        final Set<ComponentScan> scans = new HashSet<>();
        beanFactory.getBeansWithAnnotation(ComponentScan.class).forEach((name, instance) -> {
            scans.addAll(
                    AnnotatedElementUtils.getMergedRepeatableAnnotations(instance.getClass(), ComponentScan.class)
            );
        });

        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);

        scanner.addIncludeFilter(new AnnotationTypeFilter(SmartClientHandler.class));

        for (ComponentScan cs: scans) {
            for (String p: cs.basePackages()) {
                for (BeanDefinition bd : scanner.findCandidateComponents(p)) {
                    final Class clazz;
                    try {
                        clazz = Class.forName(bd.getBeanClassName());
                    } catch (ClassNotFoundException e) {
                        registerFailedClass(
                                this.classesSkipped,
                                bd.getBeanClassName(),
                                "Class '%s' does not found and will be skipped"
                                        .formatted(bd.getBeanClassName()),
                                e
                        );

                        continue;
                    }

                    if (AnnotatedElementUtils.isAnnotated(clazz, Entity.class) ) {
                        entitiesFound.add(clazz);
                    } else {
                        registerFailedClass(
                                this.classesSkipped,
                                clazz.getCanonicalName(),
                                "Class '%s' does not marked with @Entity annotation and will be skipped"
                                        .formatted(clazz.getCanonicalName()),
                                null
                        );

                        continue;
                    }
                }
            }
        }

        // -- sort in accordance to the provided order, if any
        entitiesFound.sort( (e1,e2) -> {
            final SmartClientHandler sch1 = (SmartClientHandler) e1.getAnnotation(SmartClientHandler.class);
            final SmartClientHandler sch2 = (SmartClientHandler) e2.getAnnotation(SmartClientHandler.class);
            int o1 = sch1 != null ? sch1.loadOrder() : 0;
            int o2 = sch2 != null ? sch2.loadOrder() : 0;

            return Integer.compare(o1, o2);
        });

        final String msg = "\nSmartClient JPA handler scan was completed, %d persistable entities were found, %d will be skipped:\n"
                .formatted(entitiesFound.size() + classesSkipped.size(), classesSkipped.size()) +

                entitiesFound.stream()
                    .map(Class::getCanonicalName)
                    .collect(Collectors.joining("\n   ")) +

                classesSkipped.stream()
                        .map( s -> "\n   [Skipped] %s, reason: %s".formatted(s.one, s.two))
                        .collect(Collectors.joining());

        logger.debug(msg);

        // -- register
        final HashSet<Class>  registrationFailed = new HashSet<>();

        for (Class c :entitiesFound) {
            try {
                registerJpaEntity(c);
            } catch (Throwable t) {
                registerFailedClass(
                        this.classesFailed,
                        c.getCanonicalName(),
                        "Class '%s' failed to be registered and will be skipped"
                                .formatted(c.getCanonicalName()),
                        t
                );

                registrationFailed.add(c);
            }
        }

        final int ignoredQnt =  classesSkipped.size() + classesFailed.size();
        final String msg2 = "\nSmartClient JPA Handler Registration was completed, %d persistable entities were registered and  %d were ignored:\n"
                .formatted(entitiesFound.size() - classesFailed.size(), ignoredQnt) +

                entitiesFound.stream()
                        .map(Class::getCanonicalName)
                        .collect(Collectors.joining("\n   ")) +

                classesSkipped.stream()
                        .map( s -> "\n   [Skipped] %s, reason: %s".formatted(s.one, s.two))
                        .collect(Collectors.joining()) +

                classesFailed.stream()
                        .map( s -> "\n   [Failed] %s, reason: %s".formatted(s.one, s.two))
                        .collect(Collectors.joining());

        if ( ignoredQnt <= 0) {
            logger.info(msg2);
        } else {
            logger.warn(msg2);
        }
    }

    protected  JpaDSDispatcher getJpaDSDispatcher() throws Exception {
        IDSDispatcher dsDispatcher = getTargetObject(getDsDispatcher());
        Assert.isInstanceOf(JpaDSDispatcher.class, dsDispatcher);
        final JpaDSDispatcher jpaDSDispatcher = (JpaDSDispatcher) dsDispatcher;
        return jpaDSDispatcher;
    }

    @SuppressWarnings({"unchecked"})
    protected static <T> T getTargetObject(Object proxy) throws Exception {
        while( (AopUtils.isJdkDynamicProxy(proxy))) {
            return (T) getTargetObject(((Advised)proxy).getTargetSource().getTarget());
        }
        return (T) proxy; // expected to be cglib proxy then, which is simply a specialized class
    }

    private void registerFailedClass(Collection<Tuple2<String, String>> collection, String clazz, String reason, Throwable t) {
        final Tuple2<String, String> failed = Tuple2.create( clazz, reason);
        collection.add(failed);
        logger.warn(failed.two, t);
    }


    private record Tuple2<T1, T2>( T1 one, T2 two){
        public static <T1, T2> Tuple2<T1, T2> create(T1 one, T2 two) {
            return new Tuple2<>(one, two);
        }
    }
}
