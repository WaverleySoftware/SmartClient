package org.srg.smartclient.annotations;

import java.lang.annotation.*;

@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SmartClientHandler {
    String value() default "";
    int loadOrder() default 0;
    String serverConstructor() default "";

    boolean canDelete() default false;
}
