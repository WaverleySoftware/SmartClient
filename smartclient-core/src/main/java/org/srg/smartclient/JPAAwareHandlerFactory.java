package org.srg.smartclient;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.srg.smartclient.annotations.SmartClientField;
import org.srg.smartclient.isomorphic.DSField;
import org.srg.smartclient.isomorphic.DataSource;

import javax.persistence.*;
import javax.persistence.metamodel.*;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.util.*;
import java.util.stream.Collectors;

public class JPAAwareHandlerFactory extends JDBCHandlerFactory {
    private static Logger logger = LoggerFactory.getLogger(JPAAwareHandlerFactory.class);

    public JDBCHandler createHandler(EntityManagerFactory emf, JDBCHandler.JDBCPolicy jdbcPolicy,
                                     IDSRegistry dsRegistry, Class<?> entityClass) {

        logger.trace("Building DataSource definition for JPA entity '%s'..."
                .formatted(
                        entityClass.getCanonicalName()
                )
        );

        final Metamodel mm = emf.getMetamodel();
        final DataSource ds = this.describeEntity(mm, entityClass);

        if (logger.isDebugEnabled()) {

            String dsDefinition;
            try {
                dsDefinition = DSDeclarationBuilder.build(dsRegistry, "<URL-PLACE-HOLDER>", ds, true);
            } catch (Exception e) {
                dsDefinition = "Can't serialize Data Source definition, unexpected error occurred: %s"
                        .formatted(
                                e.getMessage()
                        );

                logger.warn(dsDefinition, e);
            }

            logger.debug("DataSource definition for entity '%s' has been built:\n%s"
                    .formatted(
                            entityClass.getCanonicalName(),
                            dsDefinition
                    )
            );
        }

        logger.trace("Creating JDBCHandler Handler for JPA entity '%s'..."
                .formatted(
                        entityClass.getCanonicalName()
                )
        );

        JDBCHandler handler = createJDBCHandler(jdbcPolicy, dsRegistry, ds);
        return handler;
    }

    protected <T> DataSource describeEntity(Metamodel mm, Class<T> entityClass) {

        final Entity[] annotations =  entityClass.getAnnotationsByType(Entity.class);

        if (annotations.length == 0) {
            throw new IllegalStateException("Class '%s' does not marked  by @Entity");
        }

        final DataSource ds  = super.describeEntity(entityClass);

        ds.setTableName(
                sqlTableName(entityClass)
        );

        final EntityType<T> et = mm.entity(entityClass);
        final Set<Attribute<? super T, ?>> attrs = et.getAttributes();

        final List<DSField> fields = new ArrayList<>(attrs.size());
        final Map<String, Attribute<? super T, ?>> skippedAttrs = new HashMap<>(attrs.size());

        for (Attribute<? super T, ?> a :attrs) {
            DSField f = describeField(mm, ds.getId(), et,  a);
            if (f == null) {
                // TODO: add proper logging
                skippedAttrs.put(a.getName(), a);
                continue;
            }
            fields.add(f);

            // --  includeFrom
            /**
             *
             * Indicates this field should be fetched from another, related DataSource.
             * The includeFrom attribute should be of the form "dataSourceId.fieldName", for example:
             *
             *     <field includeFrom="supplyItem.itemName"/>
             *
             * https://www.smartclient.com/smartclient-release/isomorphic/system/reference/?id=attr..DataSourceField.includeFrom
             */
            if ( (f.getForeignKey() != null && !f.getForeignKey().isBlank())
                    && (f.getForeignDisplayField() != null && !f.getForeignDisplayField().isBlank() )) {

                final EntityType<T> targetEntity;

                if (a instanceof SingularAttribute) {
                    SingularAttribute sa = (SingularAttribute) a;
                    targetEntity = (EntityType<T>) sa.getType();
                } else if (a instanceof PluralAttribute) {
                    PluralAttribute pa = (PluralAttribute) a;
                    targetEntity = (EntityType<T>) pa.getElementType();
                } else  {
                    throw new IllegalStateException("Unsupported Attribute class '%s'".formatted(a.getClass()));
                }

                final Class<?> targetClass = targetEntity.getJavaType();
                final DSField targetField = new DSField();

                // -- detect target field type
                {
                    final DSField tf;

                    /**
                     *  Handles a case when ForeignDisplayField is @Transient, that can happen if it is a calculated field:
                     *  <pre>
                     *     @SmartClientField(hidden = true, customSelectExpression = "CONCAT(employee.last_Name, ' ', employee.first_Name)")
                     *     @Transient
                     *     private String fullName;
                     *  </pre>
                     *
                     */
                    final Field javaTargetField = FieldUtils.getField(targetClass, f.getForeignDisplayField(), true);
                    if (javaTargetField == null) {
                        throw new IllegalStateException(
                                "Datasource '%s', field '%s': Nothing known about foreignDisplayField '%s' in the target java class '%s'"
                                        .formatted(ds.getId(),
                                                f.getName(),
                                                f.getForeignDisplayField(),
                                                targetClass.getCanonicalName()
                                        )
                        );
                    }

                    if (javaTargetField.isAnnotationPresent(Transient.class)) {
                        tf = describeField(ds.getId(), javaTargetField);
                    } else {
                        final Attribute<? super T, ?> targetDisplayFieldAttr = targetEntity.getAttribute(f.getForeignDisplayField());
                        assert targetDisplayFieldAttr != null;

                        tf = describeField(mm, ds.getId(), targetEntity, targetDisplayFieldAttr);
                    }

                    targetField.setType(tf.getType());
                }

                // --
                final String targetDsId = getDsId(targetClass);

                String displayField = f.getDisplayField();
                if (displayField == null || displayField.isBlank()) {
                    displayField = "%s%s".formatted(
                            targetClass.getSimpleName().toLowerCase(),
                            StringUtils.capitalize(f.getForeignDisplayField())
                    );

                    f.setDisplayField( displayField );
                }

                targetField.setName(displayField);
                targetField.setIncludeFrom("%s.%s"
                        .formatted(targetDsId, f.getForeignDisplayField())
                );

                targetField.setIncludeVia(f.getName());


                targetField.setDbName("%s.%s"
                        .formatted(targetClass.getSimpleName(), f.getForeignDisplayField())
                );

                // by default  includeFrom field is not editable
                targetField.setCanEdit(false);

                // by default  includeFrom field is not visible
                targetField.setHidden(true);

                fields.add(targetField);
            }
        }

        // -- populate data source with JPA @Transient Fields
        final List<DSField> oldFields = ds.getFields();
        for (DSField dsf :oldFields) {
            if (!fields.contains(dsf)
                    && !skippedAttrs.containsKey(dsf.getName())) {
                // since it is @Transient field and JPA does not have any clue about it's processing, -
                // the field will be exclueded from SQL generation
                fields.add(dsf);
            }
        }
        ds.setFields(fields);
        return ds;
    }

    protected <T> DSField describeField( Metamodel mm, String dsId, EntityType<? super T> entityType, Attribute<? super T, ?> attr) {
        final Field field = (Field) attr.getJavaMember();

        // -- Generic
        final DSField f = describeField(dsId, field);

        // -- JPA
        final boolean attributeBelongsToCompositeId = !entityType.hasSingleIdAttribute()
                && entityType.getIdClassAttributes().contains(attr);

        if (attributeBelongsToCompositeId) {
            /**
             * Since attribute is a part of the composite Id/PK, it is possible that @SmartClientField annotation
             * was put on the Entity field, rather than on the IdClass field, and,  in this case,
             * generic method JDBCHandlerFactory#describeField(dsId, field)  will not find the annotation and
             * do not apply it.
             *
             * Therefore, it is required to check the entity field for @SmartClientField and apply it if it exists
             */
            SmartClientField sfa = field.getAnnotation(SmartClientField.class);

            if (sfa == null) {
                /**
                 * Ok, it is clear that IdClass field is not annotated with @SmartClientField and it is HIGHLY possible that
                 * annotation was put at the correspondent entity field.
                 *
                 * I can't find any suitable JPA MetaModel API that returns attributes for the Entity,
                 * all of them returns attributes for the IdClass. The only way to get correspondent entity fields
                 * is to use Java Reflection API.
                 */
                final Class entityJavaType = entityType.getJavaType();

                try {
                    final Field entityField = entityJavaType.getDeclaredField(attr.getName());
                    sfa = entityField.getAnnotation(SmartClientField.class);
                } catch (NoSuchFieldException e) {
                    throw new IllegalStateException(e);
                }

                if (sfa != null) {
                    applySmartClientFieldAnnotation(sfa, f);
                }
            }
        }

        final JpaRelation jpaRelation = describeRelation(mm, attr);

        if (jpaRelation != null
                && jpaRelation.joinColumns.size() == 1) {
            final JoinColumn joinColumnAnnotation = jpaRelation.joinColumns.get(0);
            if (!joinColumnAnnotation.name().isBlank()) {
                f.setDbName(joinColumnAnnotation.name());
            }
        }

        // --

        final DSField.FieldType preserveType = f.getType();
        if (attr instanceof SingularAttribute sa) {
            f.setPrimaryKey(sa.isId());

            if (f.isHidden() == null ) {
                f.setHidden( f.isPrimaryKey());
            }

            if (f.isPrimaryKey()) {
                f.setCanEdit(false);
                f.setRequired(true);
            } else {
                f.setRequired(!sa.isOptional());
            }

            final Type<T> type = sa.getType();
            final Attribute.PersistentAttributeType pat = attr.getPersistentAttributeType();

            switch (type.getPersistenceType()) {
                case BASIC:
                    f.setType(
                            fieldType(sa.getType())
                    );
                    break;

                case ENTITY:
                    switch (pat){
                        case MANY_TO_ONE:
                        case ONE_TO_ONE:
                            assert jpaRelation != null;
                            final Set<DSField> dsIdFields = getDSIDField(mm, type.getJavaType());

                            final DSField fff =  dsIdFields.iterator().next();

                            // -- foreign key
                            final String foreignDataSourceId = getDsId(type.getJavaType());
                            final String foreignFieldName;
                            if (!jpaRelation.joinColumns.isEmpty()){
                                if (jpaRelation.joinColumns.size() >1) {
                                    throw new IllegalStateException("Should be implemeted soon");
                                } else {
                                    foreignFieldName = fff.getName();
                                }
                            } else {
                                /**
                                 * As soon as entity does not declare any mapping for the attribute,
                                 * by default
                                 */
                                foreignFieldName = jpaRelation.mappedByFieldName;
                            }

                            if ( foreignFieldName == null
                                    || foreignFieldName.isBlank()) {
                                throw new IllegalStateException();
                            }

                            f.setForeignKey(
                                "%s.%s"
                                    .formatted(
                                            foreignDataSourceId,
                                            foreignFieldName
                                    )
                            );

                            // -- field type

                            if ((f.getForeignDisplayField() == null
                                    || f.getForeignDisplayField().isBlank()) && jpaRelation.joinColumns.isEmpty() ) {

                                /**
                                 * As soon as entity does not declare any mapping for the attribute,
                                 * by default entire foreign entity  will be fetched
                                 */
                                f.setType(DSField.FieldType.ENTITY);
                            } else {
                                f.setType(fff.getType());
                            }
                            break;

                        default:
                            throw new IllegalStateException("Unsupported PersistentAttributeType '%s'.".formatted(
                                    pat
                            ));
                    }
                    break;

                default:
                    Utils.throw_it("Unsupported Persistence Type %s.", type.getPersistenceType());
                    break;

            }
        } else if (attr instanceof PluralAttribute pa){
            final Type<T> type = pa.getElementType();
            final Attribute.PersistentAttributeType pat = attr.getPersistentAttributeType();

            switch (type.getPersistenceType()) {
                case ENTITY:
                    switch (pat) {
                        case ONE_TO_MANY:
                            final String s = getDsId(type.getJavaType());
                            final Class<?> javaType = type.getJavaType();
                            f.setMultiple(true);

                            // should be hidden by default
                            if (f.isHidden() == null) {
                                f.setHidden(true);
                            }

                            final Set<DSField> dsIdFields =  getDSIDField(mm, javaType);

                            DSField fff = null;
                            if (dsIdFields.size() == 1) {
                                fff = dsIdFields.iterator().next();

                            } else if (jpaRelation.mappedByFieldName != null
                                    && !jpaRelation.mappedByFieldName.isBlank()) {

                                for (DSField dsf: dsIdFields) {
                                    if (dsf.getName().equals(jpaRelation.mappedByFieldName)) {
                                        fff = dsf;
                                        break;
                                    }
                                }
                            }

                            if (fff == null) {
                                throw new IllegalStateException(
                                        "Datasource '%s', field '%s': Can't determine a foreignKey field  for '%s.%s'"
                                                .formatted( dsId,
                                                        f.getName(),
                                                        f.getForeignDisplayField(),
                                                        attr.getDeclaringType(),
                                                        attr.getName()
                                                )
                                );
                            }

                            f.setForeignKey(
                                    "%s.%s"
                                            .formatted(
                                                    getDsId(javaType),
                                                    fff.getName()
                                            )
                            );

                            f.setType(DSField.FieldType.ENTITY);
                            break;

                        default:
                            return null;
//                            throw new IllegalStateException("Unsupported PersistentAttributeType '%s'.".formatted(
//                                    pat
//                            ));
                    }
                    break;

                default:
                    Utils.throw_it("Unsupported Persistence Type %s.", type.getPersistenceType());
            }
        } else {
            Utils.throw_it("Unsupported Attribute Type %s.", attr.getClass());
        }

        if (preserveType != null) {
            f.setType(preserveType);
        }

        return f;
    }

    protected static String sqlTableName(Class<?> entityClass) {
        String tableName = entityClass.getSimpleName();

        // Convert table name to snake case
        tableName = tableName
                .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                .replaceAll("([a-z])([A-Z])", "$1_$2")
                .toLowerCase();

        // -- check entity name
        final Entity[] entities =  entityClass.getAnnotationsByType(Entity.class);

        if (entities.length != 1){
            throw new IllegalStateException("Class '%s' does not marked  by @Entity");
        }

        final Entity entity = entities[0];
        if (!entity.name().isBlank()){
            tableName = entity.name();
        }

        // -- check @Table
        final Table[] tables = entityClass.getAnnotationsByType(Table.class);
        if (tables.length == 1
                && !tables[0].name().isBlank()) {
            final Table table = tables[0];
            tableName = table.name();
        }

        return tableName;
    }

    protected <X> DSField.FieldType fieldType(Type<X> type) {
        final Class<X> clazz = type.getJavaType();
        return fieldType(clazz);
    }

    protected  <E> Set<DSField> getDSIDField( Metamodel mm, Class<E> clazz) {
        final EntityType<E> et = mm.entity(clazz);
        final ManagedType<E> mt = mm.managedType(clazz);

        if ( et.hasSingleIdAttribute() ) {
            final Attribute<? super E, ?> idAttribute = et.getId(et.getIdType().getJavaType());

            if (idAttribute == null) {
                // No sure is it possible or not
                throw new IllegalStateException("It seems there is no any @Id field, JPA entity '%s'."
                        .formatted(clazz.getCanonicalName())
                );
            }


            switch (idAttribute.getPersistentAttributeType()) {
                case BASIC:
                    return Collections.singleton(
                            describeField(mm, "<>", et, idAttribute)
                    );

                default:
                    throw new IllegalStateException("Unsupported @Id type '%s', JPA entity '%s'."
                            .formatted(
                                    idAttribute.getPersistentAttributeType(),
                                    clazz.getCanonicalName())
                    );
            }
        } else {
            //Type<?> idType = et.getIdType();
            //assert idType != null;
            final Set<SingularAttribute<? super E, ?>> idAttributes = et.getIdClassAttributes();

            if (idAttributes != null && !idAttributes.isEmpty()) {
                assert idAttributes != null;

                final Set<DSField> ids = idAttributes.stream()
                        .map(sa -> describeField(mm, "<>", et, sa))
                        .collect(Collectors.toSet());

                return ids;
            }

            throw new IllegalStateException("Can't determine id attributes for JPA entity '%s'."
                    .formatted(
                            clazz.getCanonicalName()
                    )
            );
        }
    }


    // https://www.smartclient.com/smartclient-release/isomorphic/system/reference/?id=group..jpaHibernateRelations
    private static <T> JpaRelation describeRelation(Metamodel mm, Attribute<? super T, ?> attribute) {
        if (!attribute.isAssociation()) {
            return null;
        }
        final Attribute.PersistentAttributeType pat = attribute.getPersistentAttributeType();
        final JpaRelationType relationType = JpaRelationType.from(pat);

        final EntityType<T> entityType = (EntityType<T>) attribute.getDeclaringType();
        final Field javaField = (Field) attribute.getJavaMember();

        // -- Mapped by
        final EntityType<T> mappedByEntity;
        final Attribute<? super T,?> mappedByAttribute;
        final Field mappedByField;
        final String mappedByFieldName = determineMappedBy(mm, relationType, javaField);
        if (mappedByFieldName != null && !mappedByFieldName.isBlank()) {
            if (attribute instanceof SingularAttribute sa) {
                mappedByEntity = (EntityType<T>) sa.getType();
            } else if (attribute instanceof PluralAttribute pa) {
                mappedByEntity = (EntityType<T>) pa.getElementType();
            } else  {
                throw new IllegalStateException("Attribute '%s.%s' has unsupported attribute implementation class '%s'."
                        .formatted(
                                attribute.getDeclaringType(),
                                attribute.getName(),
                                attribute.getClass()
                        )
                );
            }

            mappedByAttribute = mappedByEntity.getAttribute( mappedByFieldName );

            final Object o = mappedByAttribute.getJavaMember();
            if (o instanceof Field ) {
                mappedByField = (Field) o;
            } else {
                throw new IllegalStateException("");
            }
        } else {
            mappedByEntity = null;
            mappedByField = null;
            mappedByAttribute = null;
        }

        // -- Join columns
        List<JoinColumn> joinColumns = determineJoinColumns(javaField);

        if (joinColumns.isEmpty()) {
            joinColumns = determineJoinColumns(entityType);
        }

        List<JoinColumn> mappedByJoinColumns = mappedByField == null ? Collections.EMPTY_LIST : determineJoinColumns(mappedByField);

        if (mappedByJoinColumns.isEmpty()
                && mappedByFieldName != null
                && !mappedByFieldName.isBlank()) {
            mappedByJoinColumns = determineJoinColumns(mappedByEntity);

        }

        if (joinColumns.isEmpty() && mappedByJoinColumns.isEmpty()) {
            throw new IllegalStateException("Cant't build JpaRelation for '%s.%s': join column is not found."
                .formatted(attribute.getDeclaringType(), attribute.getName()));
        }

        return new JpaRelation(relationType, null, joinColumns, mappedByFieldName, mappedByJoinColumns);
    }

    private static String determineMappedBy(Metamodel mm, JpaRelationType type, Field field) {
        final String mappedBy = switch (type) {
            case ONE_TO_MANY -> field.getAnnotation(OneToMany.class).mappedBy();
            case ONE_TO_ONE -> field.getAnnotation(OneToOne.class).mappedBy();
            case MANY_TO_MANY -> field.getAnnotation(ManyToMany.class).mappedBy();

            // manyToOne does not support mappedBy
            case MANY_TO_ONE -> null;
            default -> null;
        };

        return mappedBy;
    }

    private static <T> List<JoinColumn> determineJoinColumns(EntityType<T> entityType) {
        final List<JoinColumn> joinColumns;

        if (!entityType.hasSingleIdAttribute()) {
            /**
             * Entity has a composite key, therefore it is also require to look for @JoinColumn annotations
             * at the MappedBy entity, if any
             */
            final Set<SingularAttribute<? super T, ?>> idAttr = entityType.getIdClassAttributes();

            joinColumns = idAttr.stream()
                    .filter(a -> a.isAssociation())
                    .map(a -> {
                        final Member jm = a.getJavaMember();
                        List<JoinColumn> jc = determineJoinColumns((Field) jm);

                        if (jc.isEmpty()) {
                            /**
                             * it seems that  Entity IdClass is not annotated, and it is highly possible that
                             * all the annotations were put at the correspondent entity fields.
                             *
                             * I can't find any JPA MetaModel API that returns attributes for the Entity,
                             * all of them returns attributes for the IdClass. Unfortunately, as a result, -
                             * the only way to get correspondent entity fields is to use a Java Reflection API.
                             */
                            final Class entityJavaType = entityType.getJavaType();

                            Field entityField = null;
                            try {
                                entityField = entityJavaType.getDeclaredField(a.getName());
                            } catch (NoSuchFieldException e) {
                            }

                            if (entityField != null) {
                                assert !jm.equals(entityField);
                                jc = determineJoinColumns(entityField);
                            }
                        }
                        assert jc != null;
                        return jc;
                    })
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        } else {
            joinColumns = Collections.EMPTY_LIST;
        }

        return joinColumns;
    }

    private static final List<JoinColumn> determineJoinColumns(Field field) {
        final List<JoinColumn> joinColumns;

        final JoinColumn joinColumnAnnotation = field.getAnnotation(JoinColumn.class);
        if (joinColumnAnnotation != null) {
            joinColumns = Collections.singletonList(joinColumnAnnotation);
        } else {
            final JoinColumns joinColumnsAnnotation = field.getAnnotation(JoinColumns.class);
            if (joinColumnsAnnotation != null){
                joinColumns = Arrays.asList(joinColumnsAnnotation.value());
            } else {
                final JoinTable joinTableAnnotation = field.getAnnotation(JoinTable.class);
                if (joinTableAnnotation != null) {
                    joinColumns = Arrays.asList(joinTableAnnotation.joinColumns());
                } else {
                    joinColumns = Collections.EMPTY_LIST;
                }
            }
        }

        return joinColumns;
    }

    protected enum JpaRelationType {
        BASIC,
        ONE_TO_MANY,
        ONE_TO_ONE,
        MANY_TO_ONE,
        MANY_TO_MANY;

        public static JpaRelationType from(Attribute.PersistentAttributeType pat) {
            return switch (pat) {
                case BASIC -> JpaRelationType.BASIC;

                case MANY_TO_ONE -> JpaRelationType.MANY_TO_ONE;
                case ONE_TO_MANY -> JpaRelationType.ONE_TO_MANY;
                case ONE_TO_ONE -> JpaRelationType.ONE_TO_ONE;
                case MANY_TO_MANY ->  JpaRelationType.MANY_TO_MANY;

                default -> throw new IllegalStateException();
            };
        }
    }

    protected static record JpaRelation(
            JpaRelationType type,

            //
            String idClassName,

            List<JoinColumn> joinColumns,

            String mappedByFieldName,
            List<JoinColumn> mappedByJoinColumn
    ){ }
}
