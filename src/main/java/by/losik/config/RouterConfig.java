package by.losik.config;

import io.vertx.ext.web.Router;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

public class RouterConfig {
    @Contract(" -> new")
    public static synchronized @NotNull Router createRouter() {
        return Router.router(VertxConfig.vertx());
    }
}
