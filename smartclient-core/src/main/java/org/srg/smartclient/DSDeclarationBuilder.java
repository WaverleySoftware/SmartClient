package org.srg.smartclient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.srg.smartclient.isomorphic.DSField;
import org.srg.smartclient.isomorphic.DataSource;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author srg
 */
abstract class DSDeclarationBuilder {

    private DSDeclarationBuilder() {}

    private static final Logger logger = LoggerFactory.getLogger(DSDeclarationBuilder.class);

    private static class BuilderContext extends RelationSupport {
        private String dsName;
        private int qntGeneratedFields;
        private StringBuilder builder;
        private final IDSRegistry dsRegistry;
        private final DataSource dataSource;

        public BuilderContext(IDSRegistry dsRegistry, DataSource dataSource) {
            this.dsRegistry = dsRegistry;
            this.dataSource = dataSource;

            clear();
        }

        final public void clear() {
            dsName = null;
            qntGeneratedFields = 0;
            builder = new StringBuilder();
        }

        public void write(String fmt, Object... args) {
            if (args == null || args.length == 0) {
                builder.append(fmt);
            } else {
                builder.append(String.format(fmt, args));
            }
        }

        public void write_if(Boolean flag, String fmt, Object... args) {
            if (Boolean.TRUE.equals(flag)) {
                write(fmt, args);
            }
        }

        public void write_if_notBlank(String str, String fmt, Object... args) {
            if (str != null && !str.isEmpty() ) {
                write(fmt, args);
            }
        }

//        public ImportFromRelation describeImportFrom(DSField importFromField) {
//            return RelationSupport.describeImportFrom(this.dsRegistry, this.dataSource, importFromField);
//        }

        public ForeignKeyRelation describeForeignKey(DSField foreignKeyField) {
            return RelationSupport.describeForeignKey(this.dsRegistry, this.dataSource, foreignKeyField);
        }
    }

    public static String build(IDSRegistry dsRegistry, String dispatcherUrl, DSHandler dsHandler) throws ClassNotFoundException {
        return build(dsRegistry, dispatcherUrl, dsHandler.dataSource(), dsHandler.allowAdvancedCriteria());
    }

    public static String build(IDSRegistry dsRegistry, String dispatcherUrl, DataSource dataSource, boolean allowAdvancedCriteria) throws ClassNotFoundException {

        final BuilderContext ctx = new BuilderContext(dsRegistry, dataSource);

        ctx.dsName = dataSource.getId();
        final Collection<DSField> allFields = dataSource.getFields();


        ctx.write("""                        
                isc.RestDataSource.create({
                  ID: "%s",
                  dataFormat:"json",
                  dataURL:"%s",
                  dataProtocol:"postMessage",
                  dataTransport:"xmlHttpRequest",
                  disableQueuing:false,
                  useStrictJSON:true,
                  allowAdvancedCriteria: %b,
                  criteriaPolicy: "dropOnChange",
                  operationBindings:[
                    {operationType:"fetch", dataProtocol:"postMessage"},
                    {operationType:"add", dataProtocol:"postMessage"},
                    {operationType:"remove", dataProtocol:"postMessage"},
                    {operationType:"update", dataProtocol:"postMessage"}
                  ],
                  fields:[""",
                ctx.dsName,
                dispatcherUrl,
                allowAdvancedCriteria
        );

        for (DSField f : allFields) {
            buildField(ctx, f);
        }

        ctx.write("]});\n");
        return ctx.builder.toString();
    }

    protected static boolean isNotBlank( String value) {
        return value != null && !value.isEmpty();
    }

    protected static void buildField(BuilderContext ctx, DSField f) throws ClassNotFoundException {

//        final Class javaClass = Utils.classForName(f.getJavaClass());
        final boolean isSubEntity = /*(f.getFields() != null && !f.getFields().isEmpty() ) ||*/ isNotBlank(f.getForeignKey());

        DSField.FieldType ft;
        if (isSubEntity) {
            ft = f.getType();
        } else {
            try {
                ft = f.getType();

                if( ft == null ){
//                if( ft == null
//                        && (f.getIncludeFrom() == null || f.getIncludeFrom().isBlank())){
//                    ft = javatype2smartclient(javaClass);
                    throw new RuntimeException("Field '%s.%s': type is not set."
                            .formatted(ctx.dsName, f.getName())
                    );
                }
            } catch (Exception ex) {
                logger.warn(
                        String.format("FIELD '%s' WON'T EXPOSED through the Datasource '%s' due type mapping error.",
                        f.getName(),
                        ctx.dsName),
                        ex);
                return;
            }
        }

        if (ctx.qntGeneratedFields++ > 0) {
            ctx.write(",\n");
        } else {
            ctx.write("\n");
        }

        ctx.write(
                "\t\t{\n"
                + "\t\t\tname:\"%s\"\n",
                f.getName());

        ctx.write_if_notBlank(f.getTitle(),
                "\t\t\t, title:'%s'\n", f.getTitle());

//        context.write_if(f.canEdit() != null && !f.canEdit(),
//                "\t\t\t, canEdit:false\n" );
//
//        context.write_if(f.getCanFilter() !=null && !f.getCanFilter(),
//                "\t\t\t, canFilter:false\n");
//

        if (!isSubEntity) {

            if (ft != null) {
                ctx.write("\t\t\t,type:\"%s\"\n",
                        ft.name());
            }
        } else {
            if( isNotBlank(f.getForeignKey())){
                ctx.write(
                        "\t\t\t,foreignKey:\"%s\"\n",
                        f.getForeignKey());
            } else {
                throw new IllegalStateException();
//                final Set<DataSourceField> pks = DataSourceField.findPK(f.getFields());
//                final boolean isComposite = pks.size() > 1;
//
//                if (isComposite) {
//                    throw new RuntimeException(
//                            String.format("Composite FK FIELD doesn't supported yet!!! (ds '%s',field: %s ).",
//                            context.dsName,
//                            f.getName()));
//                }
//
//                context.write_if(isComposite, "{");
//
//                final String dsName = getDataSourceNameByEntityType(javaClass);
//                if( dsName == null ){
//                    logger.warn(
//                            String.format("FK FIELD '%s' in the Datasource '%s' WON'T MARKED as foreignKey.",
//                            f.getName(),
//                            context.dsName));
//                }
//                for (DataSourceField dsf : pks) {
//                    context.write_if(dsName != null,
//                            "\t\t\t,foreignKey:\"%s.%s\"\n",
//                            dsName,
//                            dsf.getName());
//                }
//
//                context.write_if(isComposite, "}");
            }

            if (DSField.FieldType.ENTITY.equals(f.getType())) {
                // Due to the SmartClient documentation,it is required to specify a  Data Source Id as a field type:
                //      "declares its "type" to be the ID of the related DataSource"
                //
                // https://www.smartclient.com/smartgwt/javadoc/com/smartgwt/client/docs/JpaHibernateRelations.html


                final RelationSupport.ForeignKeyRelation foreignKeyRelation = ctx.describeForeignKey(f);
                ctx.write("\t\t\t,type:\"%s\"\n",
                        foreignKeyRelation.foreign().dataSourceId());

            }

            ctx.write_if_notBlank(f.getDisplayField(),
                    "\t\t\t,displayField:\"%s\"\n",
                    f.getDisplayField()
            );

            ctx.write_if_notBlank(f.getForeignDisplayField(),
                    "\t\t\t,foreignDisplayField:\"%s\"\n",
                    f.getForeignDisplayField()
            );
        }

        ctx.write_if_notBlank(f.getIncludeFrom(),
                "\t\t\t,includeFrom:\"%s\"\n",
                f.getIncludeFrom()
        );

        ctx.write_if_notBlank(f.getIncludeVia(),
                "\t\t\t,includeVia:\"%s\"\n",
                f.getIncludeVia()
        );

        ctx.write_if(f.isMultiple(), "\t\t\t,multiple:true\n");

        ctx.write_if(f.getIncludeSummaryFunction() != null, "\t\t\t,includeSummaryFunction:\"%s\"\n", f.getIncludeSummaryFunction());

        ctx.write_if(f.isPrimaryKey(),
                "\t\t\t,primaryKey:true\n");

        ctx.write("\t\t\t,canEdit:%b\n", f.isCanEdit());

        ctx.write_if(Boolean.TRUE.equals(f.isHidden()),
                "\t\t\t,hidden:%b\n",
                f.isHidden()
        );

        ctx.write_if(f.getRootValue() != null, ", rootValue: %s\n", f.getRootValue());

        ctx.write_if(f.isCustomSQL(),
                "\t\t\t,customSQL:%b\n",
                    f.isCustomSQL()
        );


        ctx.write_if( f.getValidOperators() != null,
                "\t\t\t,validOperators: [%s]\n",
                f.getValidOperators() == null ? null : f.getValidOperators().stream()
                        .map(operatorId -> "\"%s\"".formatted(operatorId.jsonValue()))
                    .collect(Collectors.joining(","))
        );

//        context.write_if_notBlank(f.getCustomSelectExpression(),
//                "\t\t\t,customSelectExpression:\"%s\"\n",
//                f.getCustomSelectExpression()
//        );

//        if(f.getValueMap() != null ){
//            final Field.ValueMap vm = f.getValueMap();
//            context.write("\t\t\t, valueMap:{\n");
//            buildValueMapContent(context.builder, vm, "\t\t\t\t");
//            context.write("\t\t\t }\n");
//        }

        if (f.getValueMap() != null) {
            final String s = ((Map<Object, Object>)f.getValueMap()).entrySet()
                    .stream()
//                    .map( entry -> "{ %s:'%s'}"
//                            .formatted(entry.getKey(),
//                                    entry.getValue()
//                                            .toString()
//                                            .replaceAll("'", "\'")
//                            )
//                    )
                    .map( entry -> "'%s'"
                            .formatted(
                                    entry.getValue()
                                            .toString()
                                            .replaceAll("'", "\'")
                            )
                    )
                    .collect(Collectors.joining("\n\t\t\t   ,"));


            ctx.write("\t\t\t,valueMap:[\n\t\t\t   %s\n\t\t\t]\n", s);
        }

        ctx.write("\t\t}");
    }
}
