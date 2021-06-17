package org.srg.smartclient;

import org.srg.smartclient.isomorphic.DSRequest;
import org.srg.smartclient.isomorphic.OperationBinding;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class SQLAddContext<H extends JDBCHandler> extends JDBCHandler.AbstractSQLContext {

    private String addSQL;

    private List<JDBCHandler.IFilterData> filterData;

    public SQLAddContext(H dsHandler, DSRequest request, OperationBinding operationBinding) throws Exception {
        super(dsHandler, request, operationBinding);
        init();
    }

    protected void init() {

        final Predicate<String> exclusionPredicate = createCriteriaExclusionPredicate(
                operationBinding() != null? operationBinding().getExcludeCriteriaFields() : null);

        filterData = dsHandler().generateFilterData(
                DSRequest.OperationType.ADD,
                DSRequest.TextMatchStyle.EXACT,
                request().getData(),
                exclusionPredicate
        ).stream().filter(it -> {
            assert it instanceof JDBCHandler.FilterData;
            return ((JDBCHandler.FilterData) it).getDsFieldPair().field().isPrimaryKey()
                    || ((JDBCHandler.FilterData) it).getDsFieldPair().dataSource().equals(this.dsHandler().dataSource());
        }).collect(Collectors.toList());

        final String insertSQL = filterData.stream()
                .map(fd -> {
                    assert fd instanceof JDBCHandler.FilterData;
                    return ((JDBCHandler.FilterData) fd).getDsFieldPair().getSqlFieldAlias();
                })
                .collect(Collectors.joining(", "));
        final String valuesSQL = filterData.stream()
                .map(fd -> "?")
                .collect(Collectors.joining(", "));
        this.addSQL = """
                        INSERT INTO  %s 
                            (%s)
                        VALUES (%s);                            
                    """.formatted(
                dataSource().getTableName(),
                insertSQL,
                valuesSQL
        );

    }

    public String getAddSQL() {
        return addSQL;
    }

    public List<JDBCHandler.IFilterData> getFilterData() {
        return filterData;
    }
}
