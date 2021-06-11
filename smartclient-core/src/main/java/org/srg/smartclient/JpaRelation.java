package org.srg.smartclient;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.srg.smartclient.utils.AnnotationUtils;

import javax.persistence.*;
import javax.persistence.metamodel.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;
import java.util.stream.Collectors;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
@JsonIgnoreProperties(ignoreUnknown = true)
public record JpaRelation<S, T>(
        JpaRelationType type,

        //
        String idClassName,

        IdentifiableType<S> sourceType,
        Attribute<S,?> sourceAttribute,

        IdentifiableType<T>  targetType,

        /**
         * It can be null, if describes a uni-directional relation
         */
        Attribute<T,?> targetAttribute,

        /**
         * <code>true</code> if describes a backward side of a relation
         */
        boolean isInverse,

        List<JoinColumn> joinColumns,

        JoinTable joinTable
) {

    @JsonIgnore
    public String getTargetAttributeName_or_null() {
        if (targetAttribute != null) {
            return targetAttribute.getName();
        }
        return null;
    }

    public enum JpaRelationType {
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
                case MANY_TO_MANY -> JpaRelationType.MANY_TO_MANY;

                default -> throw new IllegalStateException();
            };
        }
    }

    // https://www.smartclient.com/smartclient-release/isomorphic/system/reference/?id=group..jpaHibernateRelations
    public static <T, Tt> JpaRelation<T, Tt> describeRelation(Metamodel mm, EntityType<? super T> sourceEntityType, Attribute<?, ?> sourceAttribute) {

        if (!sourceAttribute.isAssociation()) {
            return null;
        }

        final Attribute.PersistentAttributeType pat = sourceAttribute.getPersistentAttributeType();
        final JpaRelationType relationType = JpaRelationType.from(pat);

        final Field sourceJavaField = (Field) sourceAttribute.getJavaMember();

        // -- determine target java type
        final Class<?> targetJavaType;
        if (sourceAttribute.isCollection()) {
            final java.lang.reflect.Type genericFieldType = sourceJavaField.getGenericType();
            if (genericFieldType instanceof ParameterizedType) {
                final ParameterizedType aType = (ParameterizedType) genericFieldType;
                final java.lang.reflect.Type[] fieldArgTypes = aType.getActualTypeArguments();

                if (fieldArgTypes.length != 1) {
                    throw new IllegalStateException();
                }

                targetJavaType = (Class<?>) fieldArgTypes[0];
            } else {
                targetJavaType = sourceAttribute.getJavaType();
            }
        } else {
            targetJavaType = sourceAttribute.getJavaType();
        }

        final EntityType<?> targetEntityType = mm.entity(targetJavaType);

        final String mappedByFieldName = determineMappedBy(mm, relationType, sourceJavaField);

        final boolean isInverseRelation = mappedByFieldName != null && !mappedByFieldName.isBlank();

        // -- Mapped by
        final EntityType<T> mappedByEntity;
        final Attribute<? super T, ?> mappedByAttribute;
        final Field mappedByField;

        if (mappedByFieldName != null && !mappedByFieldName.isBlank()) {
            if (sourceAttribute instanceof SingularAttribute sa) {
                mappedByEntity = (EntityType<T>) sa.getType();
            } else if (sourceAttribute instanceof PluralAttribute pa) {
                mappedByEntity = (EntityType<T>) pa.getElementType();
            } else {
                throw new IllegalStateException("Attribute '%s.%s' has unsupported attribute implementation class '%s'."
                        .formatted(
                                sourceEntityType.getName(),
                                sourceAttribute.getName(),
                                sourceAttribute.getClass()
                        )
                );
            }

            mappedByAttribute = mappedByEntity.getAttribute(mappedByFieldName);

            final Object o = mappedByAttribute.getJavaMember();
            if (o instanceof Field) {
                mappedByField = (Field) o;
            } else {
                throw new IllegalStateException("");
            }
        } else {
            mappedByEntity = null;
            mappedByField = null;
            mappedByAttribute = null;
        }

        // -- Join table (many to many)
        final JoinTable joinTableAnnotation = getAnnotation(JoinTable.class, sourceAttribute);

        // -- Join columns
        final List<AssociationOverride> associationOverrides = determineAssociationOverrides(sourceEntityType, sourceAttribute);
        final List<JoinColumn> joinColumns = determineJoinColumns(sourceEntityType, sourceAttribute);

        final List<JoinColumn> allJoinColumns = new ArrayList<>(joinColumns);
        final List<JoinTable> allJoinTables = new ArrayList<>();

        if (joinTableAnnotation != null){
            allJoinTables.add(joinTableAnnotation);
        }

        for (AssociationOverride ao : associationOverrides) {
            if (ao.joinColumns() != null
                    && ao.joinColumns().length > 0) {
                for (JoinColumn jc : ao.joinColumns()) {
                    allJoinColumns.add(jc);
                }
            }

            if (ao.joinTable() != null) {
                final JoinTable jt = ao.joinTable();
                 allJoinTables.add(jt);

                if (jt != null
                        && jt.joinColumns() != null) {

                    for (JoinColumn jc : jt.joinColumns()) {
                        allJoinColumns.add(jc);
                    }
                }
            }
        }

        final JoinColumn effectiveJoinColumn = AnnotationUtils.mergeAnnotations(JoinColumn.class, allJoinColumns);

//        List<JoinColumn> mappedByJoinColumns = mappedByField == null ? Collections.EMPTY_LIST : determineJoinColumns(mappedByField);
//
//        if (mappedByJoinColumns.isEmpty()
//                && mappedByFieldName != null
//                && !mappedByFieldName.isBlank()) {
//            mappedByJoinColumns = determineJoinColumns(mappedByEntity);
//        }
        final List<JoinColumn> mappedByJoinColumns = Collections.EMPTY_LIST;

//        if (joinColumns.isEmpty() && mappedByJoinColumns.isEmpty()) {
//            throw new IllegalStateException("Cant't build JpaRelation for '%s.%s': join column is not found."
//                    .formatted(sourceEntityType.getName(), sourceAttribute.getName()));
//        }


        Attribute<?, ?> effectiveTargetAttribute = mappedByAttribute;

        if (mappedByAttribute == null
            && !isInverseRelation) {
            final Set<SingularAttribute<?,?>> idAttributes = (Set<SingularAttribute<?, ?>>) getIdAttributes(targetEntityType);

            if (idAttributes.size() > 1) {
                throw new IllegalStateException("Composite PKs have not been supported yet.");
            }

            effectiveTargetAttribute = idAttributes.iterator().next();
        }


        // -- Join Table
        final JoinTable joinTable;

//        final JoinTable effectiveJoinTable = mergeAnnotations(JoinTable.class, DEFAULT_JOIN_TABLE, allJoinTables);
        if (mappedByAttribute == null) {
            joinTable = sourceJavaField.getAnnotation(JoinTable.class);
        } else {
            joinTable = mappedByField.getAnnotation(JoinTable.class);
        }

        return new JpaRelation(
                relationType, null,
                sourceEntityType,
                sourceAttribute,
                targetEntityType,
                effectiveTargetAttribute,
                isInverseRelation,
                effectiveJoinColumn == null ? Collections.EMPTY_LIST : List.of(effectiveJoinColumn),
                joinTable
        );
    }

    private static <T> List<AssociationOverride> determineAssociationOverrides(EntityType<? super T> sourceEntityType, Attribute<?, ?> sourceAttribute) {
        final AssociationOverrides overrides = getAnnotation(AssociationOverrides.class, sourceEntityType);
        if (overrides != null) {
            for (AssociationOverride ao : overrides.value()) {
                if (ao.name().equals(sourceAttribute.getName())) {
                    return List.of(ao);
                }
            }
        }

        final AssociationOverride ao = getAnnotation(AssociationOverride.class, sourceEntityType);
        if (ao != null) {
            return List.of(ao);
        }

        return Collections.EMPTY_LIST;
    }

    private static <T> List<JoinColumn> determineJoinColumns(EntityType<? super T> sourceEntityType, Attribute<?, ?> sourceAttribute) {
        List<JoinColumn> joinColumns = determineJoinColumns((Field) sourceAttribute.getJavaMember());

        if (joinColumns.isEmpty()) {
            joinColumns = determineJoinColumns(sourceEntityType);
        }

        return joinColumns;
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

    private static <T> List<JoinColumn> determineJoinColumns(IdentifiableType<T> entityType) {
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
            if (joinColumnsAnnotation != null) {
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

    private static <A extends Annotation> A getAnnotation(Class<A> clazz, EntityType<?> entityType ) {
        final Class c = entityType.getJavaType();
        return (A) c.getAnnotation(clazz);
    }

    private static <A extends Annotation> A getAnnotation(Class<A> clazz, Attribute<?, ?> sourceAttribute) {
        final Field f = (Field) sourceAttribute.getJavaMember();
        return f.getAnnotation(clazz);
    }

    public static <E> Set<SingularAttribute<? super E,?>> getIdAttributes(EntityType<E> et) {
        if ( et.hasSingleIdAttribute() ) {
            final SingularAttribute<? super E, ?> idAttribute = et.getId(et.getIdType().getJavaType());

            if (idAttribute == null) {
                // No sure is it possible or not
                throw new IllegalStateException("It seems there is no any @Id field, JPA entity '%s'."
                        .formatted(et.getJavaType().getCanonicalName())
                );
            }
            return Set.of(idAttribute);
        } else {
            final Set<SingularAttribute<? super E, ?>> idAttributes = et.getIdClassAttributes();

            if (idAttributes == null || idAttributes.isEmpty()) {
                throw new IllegalStateException("Can't determine id attributes for JPA entity '%s'."
                        .formatted(
                                et.getJavaType().getCanonicalName()
                        )
                );
            }
            return idAttributes;
        }
    }
}
