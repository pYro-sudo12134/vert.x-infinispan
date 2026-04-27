package by.losik.config;

import by.losik.constant.AppConstants;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.micrometer.backends.BackendRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class MetricsConfig {
    private static final Logger log = LoggerFactory.getLogger(MetricsConfig.class);
    public static void setupMetricsServer() {
        Router router = RouterConfig.router();

        router.get("/metrics").handler(ctx -> {
            PrometheusMeterRegistry registry = (PrometheusMeterRegistry) BackendRegistries.getDefaultNow();
            Optional.ofNullable((PrometheusMeterRegistry) BackendRegistries.getDefaultNow())
                    .ifPresentOrElse(prometheusMeterRegistry -> ctx.response()
                                    .putHeader("Content-Type", "text/plain; version=0.0.4")
                                    .end(registry.scrape()),
                            () -> ctx.response()
                                    .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                                    .end("Metrics registry not available"));
        });

        router.get("/metrics/json").handler(ctx -> {
            Optional.ofNullable((PrometheusMeterRegistry) BackendRegistries.getDefaultNow())
                    .ifPresentOrElse(prometheusMeterRegistry -> {
                                JsonObject response = new JsonObject()
                                        .put("status", AppConstants.STATUS_OK)
                                        .put("metricsEndpoint", "/metrics")
                                        .put("format", "prometheus");
                                ctx.response()
                                        .putHeader("Content-Type", "application/json")
                                        .end(response.encode());
                            },
                            () -> ctx.response()
                                    .setStatusCode(AppConstants.HTTP_INTERNAL_ERROR)
                                    .end(new JsonObject().put("error", "Metrics registry not available").encode()));
        });

        VertxConfig.vertx().createHttpServer()
                .requestHandler(router)
                .listen(AppConfig.metricsPort())
                .onSuccess(server -> log.info("Metrics server started on port {}", AppConfig.metricsPort()))
                .onFailure(err -> log.error("Failed to start metrics server on port {}", AppConfig.metricsPort(), err));
    }
}
