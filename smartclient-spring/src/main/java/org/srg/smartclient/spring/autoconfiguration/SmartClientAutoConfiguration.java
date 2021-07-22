package org.srg.smartclient.spring.autoconfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.*;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.MimeType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.srg.smartclient.IDSDispatcher;
import org.srg.smartclient.isomorphic.DSTransaction;
import org.srg.smartclient.utils.Serde;
import org.srg.smartclient.isomorphic.DSResponse;
import org.srg.smartclient.isomorphic.IDSRequest;
import org.srg.smartclient.spring.SmartClientProperties;
import reactor.core.publisher.Mono;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Optional;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.POST;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Smart Client.
 *
 * @see https://www.smartclient.com/smartgwtee-release/javadoc/com/smartgwt/client/docs/ServletDetails.htmlsky]
 */
@Configuration
@ConditionalOnClass({ IDSDispatcher.class, DataSource.class})
@AutoConfigureAfter(HibernateJpaAutoConfiguration.class)
@ConditionalOnBean(IDSDispatcher.class)
@EnableConfigurationProperties(SmartClientProperties.class)
@Import(SmartclientConfigurerConfiguration.class)
public class SmartClientAutoConfiguration {
    private static Logger logger = LoggerFactory.getLogger(SmartClientAutoConfiguration.class);
//    private static final String REST_URL = "/dispatcher";

    @Autowired
    private SmartClientProperties smartClientProperties;

    @Autowired
    private IDSDispatcher dsDispatcher;

    protected Mono<ServerResponse> processRequest(String request, String fileName, boolean byDatasourceConfig) {

        final IDSRequest dsRequest;
        try {
            dsRequest = Serde.deserializeRequest(request);
        } catch(Exception ex) {
            logger.error("Can't deserialize ds request: \n%s"
                            .formatted(request),
                    ex);

            return ServerResponse
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .cacheControl(CacheControl.noCache())
                    .bodyValue(ex.getMessage());
        }

        final Integer transactionNum = dsRequest instanceof DSTransaction dsTransaction ? dsTransaction.getTransactionNum() : null;


        try {
            final Collection<DSResponse> responses = dsDispatcher.dispatch(dsRequest);

            if (fileName == null || fileName.isBlank()) {
                // -- Usual request processing
                try (final StringWriter sw = new StringWriter();) {
                    sw.write("<SCRIPT>//'\"]]>>isc_JSONResponseStart>>\n");

                    Serde.serializeResponse(sw, transactionNum, responses);

                    sw.write("\n//isc_JSONResponseEnd");

                    return ServerResponse
                            .ok()
                            .cacheControl(CacheControl.noCache())
                            .bodyValue(sw.toString());
                }
            } else {
                // -- export data request

                String ext = fileName.substring(fileName.lastIndexOf(".")+1);

                if (!"csv".equalsIgnoreCase(ext)) {
                    throw new RuntimeException(
                        "Unsupported export format '%s', the only format supported for export data is 'CSV'"
                            .formatted(ext)
                    );
                }

                if (responses.size() > 1) {
                    throw new IllegalStateException("Multiple responses is not supported for export data.");
                }


                final DSResponse response = responses.iterator().next();

                try(final ByteArrayOutputStream bos = new ByteArrayOutputStream();) {
                    try (final OutputStreamWriter writer = new OutputStreamWriter(bos);) {
                        Serde.serializeResponseAsCSV(writer, ',', response, byDatasourceConfig);
                        writer.flush();
                    }
//                final InputStreamResource resource =  new InputStreamResource(
//                        new ByteArrayInputStream(
//                                bos.toByteArray()
//                        )
//                );

                    // https://forums.smartclient.com/forum/smart-gwt-technical-q-a/7425-url-request-using-exportdata-in-listgrid
                    // https://medium.com/@victortemitope95/how-to-write-and-download-a-csv-file-in-spring-webflux-5df8d817a597
                    return ServerResponse
                            .ok()
                            .cacheControl(CacheControl.noCache())
                            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"")
                            .header(HttpHeaders.CONTENT_TYPE, "text/csv")
//                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_OCTET_STREAM_VALUE)
//                        .body(BodyInserters.fromResource(resource));
//                        .body(Mono.just(new ByteArrayInputStream(bos.toByteArray())));
                            .bodyValue(bos.toString());
                }
            }
        } catch (Throwable t) {
            return Mono.error(t);
        }
    }


    @Bean
    public RouterFunction<ServerResponse> smartClientRESTHandler() {
        // https://github.com/sdeleuze/webflux-multipart/blob/master/src/main/java/com/example/MultipartRoute.java
        //https://www.programcreek.com/java-api-examples/?code=hantsy/spring-reactive-sample/spring-reactive-sample-master/routes/src/main/java/com/example/demo/PostHandler.java
        return RouterFunctions.route(POST(smartClientProperties.getDispatcherPath()), r ->
             r.bodyToMono(String.class)
                    .flatMap( body -> this.processRequest(body, null, false))
        )
        // export data
        .andRoute(POST(smartClientProperties.getDispatcherPath()+"/{export-file-name}"), r -> {
            final String exportFile = r.pathVariable("export-file-name");
            final Optional<String> optionalByDatasourceConfig = r.queryParam("by-datasource-config");
            boolean byDatasourceConfig = Boolean.parseBoolean(optionalByDatasourceConfig.orElse(null));
            return r.bodyToMono(String.class)
                    .flatMap(body -> this.processRequest(body,exportFile, byDatasourceConfig));
        });
    }

    @Bean
    public RouterFunction<ServerResponse> smartClientFileDownload( ) {
        return RouterFunctions
                .resources("/isomorphic/**", new ClassPathResource("assets/isomorphic/"));
    }

    @Bean
    public RouterFunction<ServerResponse> smartClientDataSourceLoader(){
        return RouterFunctions
                .route(GET("/ds-loader"), (r) -> {
                    final String efficientUrl =  r.uriBuilder().replacePath(smartClientProperties.getDispatcherPath()).build().toString();
                    final StringBuilder sbld = new StringBuilder();
                    try {
                        dsDispatcher.generateDSJavaScript(sbld, efficientUrl);

                        if (sbld.length() == 0) {
                            sbld.append("// There is no any registered smartclient data source");
                        }

                        return ServerResponse
                                .ok()
                                .cacheControl(CacheControl.noCache())
                                .contentType(MediaType.asMediaType(MimeType.valueOf("application/javascript")))
                                .bodyValue(sbld.toString());
                    } catch (Throwable t) {
                        return Mono.error(t);
                    }
                });
    }
}
