package org.srg.smartclient.utils;

import org.jboss.jandex.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.*;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.stream.Collectors;

public final class AnnotationUtils {
    private static Logger logger = LoggerFactory.getLogger(AnnotationUtils.class);

    private AnnotationUtils() {
    }

    private static String replaceLast(String text, String regex, String replacement) {
        return text.replaceFirst("(?s)"+regex+"(?!.*?"+regex+")", replacement);
    }

    public static <A extends Annotation> A getAnnotation(Class<?> clazz, String fieldName, Class<A> annotationClass) {
        final Indexer indexer = new Indexer();
        final List<String> names = new LinkedList<>();

        for (Class c = clazz; !c.equals(Object.class); c = c.getSuperclass()) {
            final String canonicalName = c.getCanonicalName();

            String cname = canonicalName.replaceAll("\\.", "/") ;

            if (c.isMemberClass()) {
                cname = replaceLast(cname, "\\/", "\\$");
            }
            cname += ".class";

            InputStream stream = annotationClass
                    .getClassLoader()
                    .getResourceAsStream(cname);

            try {
                indexer.index(stream);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            names.add(cname);
        }

        final Index index = indexer.complete();

        final DotName dotAnnotation = DotName.createSimple(annotationClass.getCanonicalName());
        List<AnnotationInstance> annotations = index.getAnnotations(dotAnnotation);

        final BeanInfo bi;
        try {
            bi = Introspector.getBeanInfo(clazz);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }

        PropertyDescriptor propertyDescriptor = null;
        for (PropertyDescriptor pd : bi.getPropertyDescriptors()) {
            if (fieldName.equals(pd.getName())) {
                propertyDescriptor = pd;
                break;
            }
        }

        if (propertyDescriptor == null) {
            /**
             * If property descriptor is null that probably means that it is not a bean property and there is no
             * accessors for it. In most cases it will be a field introduced by a various proxies introduced by
             * Hibernate, java assist, etc.
             */

            logger.debug("Property Description is empty {}.{}, this will be treated as no {} annotation was found for the field.",
                    clazz.getSimpleName(), fieldName, annotationClass.getSimpleName());
            return null;
        }

        final List<AnnotationInstance> effectiveAnnotations = new LinkedList<>();

        final Map<String, Object> defaultValues = annotationDefaultValues(annotationClass);

        for (AnnotationInstance annotation : annotations) {
            switch (annotation.target().kind()) {
                case METHOD:
                    final String methodName = annotation.target().asMethod().name();
                    if ( (propertyDescriptor.getReadMethod() != null &&  methodName.equals(propertyDescriptor.getReadMethod().getName()))
                            || ( propertyDescriptor.getWriteMethod() != null && methodName.equals(propertyDescriptor.getWriteMethod().getName()) )
                    ) {
                        effectiveAnnotations.add(annotation);
                    }
                    break;

                case FIELD:
                    final String fName = annotation.target().asField().name();
                    if (fName.equals(propertyDescriptor.getName())) {
                        effectiveAnnotations.add(annotation);
                    }
                    break;
            }
        }

        final List<A> ea = effectiveAnnotations.stream().map( a -> {
            final Map<String, Object> values = new HashMap<>(defaultValues);

            a.values().forEach( av -> {
                switch (av.kind()) {
                    case ENUM:
                        final DotName enumClassName = av.asEnumType();
                        try {
                            final Class enumClass = Class.forName(enumClassName.toString());
                            final Object o = Enum.valueOf(enumClass, av.asEnum());
                            values.put(av.name(), o);
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        }
                        break;

                    default:
                        values.put(av.name(), av.value());
                }
            });

            return RuntimeAnnotations.annotationForMap(annotationClass, values);
        }).collect(Collectors.toList());

        final A ai = effectiveAnnotations.isEmpty() ? null : mergeAnnotations(annotationClass, ea);
        return ai;
    }

    /**
     * Compares two maps and returns changes from the right map (that does not exist in the left one).
     */
    protected static Map<String, Object> detectChanges(Map<String,Object> left, Map<String,Object> right) {
        final Map<String, Object> changes = new HashMap<>();
        for (var e : right.entrySet()) {
            final String k = e.getKey();
            final Object rv = e.getValue();
            Object ev = left.get(k);

            if (rv instanceof Map rm) {
                if (ev instanceof Annotation a) {
                    ev = annotationValues(a);
                }
                final Map<String, Object> vv = detectChanges((Map)ev, rm);

                if (!vv.isEmpty()) {
                    changes.put(k, vv);
                }
            } else if (ev != rv && !ev.equals(rv)) {
                changes.put(k, rv);
            }
        }
        return changes;
    }

    /**
     * Retrieves all annotation values.
     */
    public  static <A extends Annotation> Map<String, Object> annotationValues(A annotation) {
        final Map<String, Object> values = new HashMap<>();

        final  Method methods[] =  annotation.annotationType().getDeclaredMethods();
        for (Method m : methods) {
            try {
                Object v = null;
                v = m.invoke(annotation, (Object[]) null);
                if ( v instanceof Annotation a) {
                    v = annotationValues(a);
                }
                values.put(m.getName(), v);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        return values;
    }

    /**
     * Retrieves default annotation values for annotation specified by a given class.
     */
    public static <A extends Annotation> Map<String, Object> annotationDefaultValues(Class<A> annotationClass) {
        final Map<String, Object> values = new HashMap<>();

        // -- populate map with default values
        final BeanInfo bi;
        try {
            bi = Introspector.getBeanInfo(annotationClass);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }

        assert bi.getPropertyDescriptors().length == 0;

        final MethodDescriptor mds[] = bi.getMethodDescriptors();

        for (MethodDescriptor md: mds) {
            final Object dv = md.getMethod().getDefaultValue();
            values.put(md.getName(), dv);
        }

        return values;
    }

    /**
     * Merge a bunch of annotations into the effective one with the respect of default values. That means that:
     *
     * public @interface A {
     *     String name() default "";
     *     boolean nullable() default true;
     *     boolean insertable() default true;
     * }
     *
     * @A(name="1st"), @A(nullable=false), @A(name="2nd")
     *
     * effective will be @A(name=''2nd', nullable=false, insertable=true)
     */
    public static <A extends Annotation> A mergeAnnotations(Class<A> annotationClass, Iterable<A> annotations) {
        if (!annotations.iterator().hasNext()) {
            return null;
        }

        try {
            final Map<String, Object> defaultValues = annotationDefaultValues(annotationClass);
            final Map<String, Object> effectiveValues = new HashMap<>(defaultValues);

            for (A a : annotations) {
                final Map<String, Object> v = annotationValues(a);
                final Map<String, Object> changes = detectChanges(defaultValues, v);
                effectiveValues.putAll(changes);
            }

            if (!effectiveValues.isEmpty()) {
                return RuntimeAnnotations.annotationForMap(annotationClass, effectiveValues);
            } else {
                return null;
            }
        } catch (Throwable t) {
            if (t instanceof RuntimeException rte) {
                throw rte;
            }
            throw new RuntimeException(t);
        }
    }

}
