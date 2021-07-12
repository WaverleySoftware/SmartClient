package org.srg.smartclient;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.hibernate.annotations.SQLDelete;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.srg.smartclient.annotations.SmartClientField;
import org.srg.smartclient.annotations.SmartClientHandler;
import org.srg.smartclient.isomorphic.DSField;
import org.srg.smartclient.isomorphic.DataSource;
import org.srg.smartclient.utils.AnnotationUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JDBCHandlerFactory {
    private static Logger logger = LoggerFactory.getLogger(JDBCHandlerFactory.class);


    public JDBCHandler createJDBCHandler(JDBCHandler.JDBCPolicy jdbcPolicy, IDSRegistry dsRegistry, DataSource ds) {
        if (ds.getServerConstructor() != null) {
            try {
                final Class< ? super JDBCHandler> c = (Class<? super JDBCHandler>) Class.forName(ds.getServerConstructor());
                return (JDBCHandler) ConstructorUtils.invokeConstructor(c, jdbcPolicy, dsRegistry, ds);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return new AdvancedJDBCHandler(jdbcPolicy, dsRegistry, ds);
        }
    }

    protected <T> DataSource describeEntity(Class<T> entityClass) {
        final DataSource ds = new DataSource();
        final String id = getDsId(entityClass);
        ds.setId(id);
        ds.setServerType(DataSource.DSServerType.SQL);
        ds.setBeanClassName(entityClass.getCanonicalName());

        final SmartClientHandler a = entityClass.getAnnotation(SmartClientHandler.class);
        if (a != null && a.serverConstructor() != null && !a.serverConstructor().isBlank()) {
            ds.setServerConstructor(a.serverConstructor());
        }

        if (a != null && a.canDelete()) {
            final SQLDelete b = entityClass.getAnnotation(SQLDelete.class);
            if (b != null) {
                ds.setDeletionType(DataSource.DeletionType.SOFT_DELETE);
            } else {
               ds.setDeletionType(DataSource.DeletionType.DELETE);
            }
        } else {
            ds.setDeletionType(DataSource.DeletionType.NOT_AVAILABLE);
        }

        final List<Field> fields = FieldUtils.getAllFieldsList(entityClass);

        final List<DSField> dsFields = fields.stream()
                // remove hibernate-related internal fields (they are started with $ sign)
                .filter( f -> !f.getName().startsWith("$"))
                .map( f -> describeField(ds.getId(), entityClass, f))
                .collect(Collectors.toList());

        ds.setFields(dsFields);
        return ds;
    }

    protected <T> DSField describeField(String dsId, Class<?> entityClass, Field field) {
        final DSField f = new DSField();

        f.setName( field.getName() );

        try {
            f.setType(fieldType(field.getType()));

            if (f.getType() != null) {
                switch (f.getType()) {
                    case ENUM:
                    case INTENUM:
                        final Class<?> type = field.getType();
                        if (!type.isEnum()) {
                            throw new IllegalStateException();
                        }

                        f.setValueMapEnum(type.getCanonicalName());

                        Enum constants[] = (Enum[]) type.getEnumConstants();

                        final Map<Integer, String> vm = new HashMap<>(constants.length);

                        for (int i = 0; i < constants.length; ++i) {
                            vm.put(i, constants[i].name());
                        }

                        f.setValueMap(vm);
                        break;
                }

                // Init default OperatorIds
                f.setValidOperators(f.getType().defaultOperators);
            }
        } catch (Throwable t) {
            logger.warn("DataSource '%s': Can't determine field type for field '%s'"
                    .formatted(dsId, f.getName()), t);
            f.setType(null);
        }


        final SmartClientField sfa = getAnnotation(entityClass, field.getName(), SmartClientField.class); //field.getAnnotation(SmartClientField.class);
        applySmartClientFieldAnnotation(sfa, f);

        // -- set DB field name
        f.setDbName( f.getName());


        // Convert field name to snake case
        f.setDbName(
                f.getDbName().replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2").replaceAll("([a-z])([A-Z])", "$1_$2")
        );

        return f;
    }

    protected static <A extends Annotation> A getAnnotation(Class<?> clazz, String name, Class<A> annotationClass) {
        return AnnotationUtils.getAnnotation(clazz, name, annotationClass);
    }

    protected static void applySmartClientFieldAnnotation(SmartClientField sfa, DSField dsf) {
        if (sfa != null) {

            if (!sfa.name().isBlank()) {
                dsf.setName( sfa.name());
            }

            if (!sfa.foreignDisplayField().isBlank()) {
                dsf.setForeignDisplayField(sfa.foreignDisplayField());
            }

            if (!sfa.displayField().isBlank()) {
                dsf.setDisplayField(sfa.displayField());
            }

            if (!sfa.type().equals(DSField.FieldType.ANY)) {
                dsf.setType(sfa.type());
            }

            if (sfa.hidden()) {
                dsf.setHidden(Boolean.TRUE);
            }else {
                dsf.setHidden(Boolean.FALSE);
            }

            if (!sfa.customSelectExpression().isBlank()) {
                dsf.setCustomSelectExpression(sfa.customSelectExpression());
            }
            dsf.setCanEdit(sfa.canEdit());

        }

    }

    protected <X> DSField.FieldType fieldType(Class<X> clazz) {
        if (clazz.equals(Integer.class)
                || clazz.equals(Short.class)
                || clazz.equals(int.class)
                || clazz.equals(short.class)) {
            return DSField.FieldType.INTEGER;
        }

        if(clazz.equals(Long.class)
                || clazz.equals(long.class) ){
            return DSField.FieldType.INTEGER;
        }

        if (clazz.equals(String.class)) {
            return DSField.FieldType.TEXT;
        }

        if (clazz.equals(java.sql.Date.class)) {
            return DSField.FieldType.DATE;
        }

        if (clazz.equals(java.sql.Timestamp.class)) {
            return DSField.FieldType.DATETIME;
        }

        if( clazz.equals(Boolean.class)
                || clazz.equals(boolean.class) ){
            return DSField.FieldType.BOOLEAN;
        }

        if (clazz.isEnum()) {
            return DSField.FieldType.ENUM;
        }

        if (clazz.equals(java.sql.Time.class)) {
            return DSField.FieldType.TIME;
        }

        //throw new RuntimeException(String.format("Smart Client -- Unmapped field type %s.", clazz.getName()));
        return null;
    }

    protected static String getDsId(Class<?> clazz) {
        final SmartClientHandler[] annotations = clazz.getAnnotationsByType(SmartClientHandler.class);

        if (annotations.length > 0) {
            final SmartClientHandler smartClientHandler = annotations[0];

            if (!smartClientHandler.value().isBlank()) {
                return smartClientHandler.value();
            }
        }

        return clazz.getSimpleName() +"DS";
    }

}
