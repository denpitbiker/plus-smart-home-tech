package ru.yandex.practicum.analyzer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/** Records actual JDBC executions rather than Hibernate's generated SQL log. */
@TestConfiguration
class JdbcProbe {
    static final List<String> executions = new ArrayList<>();
    static int batches;

    static void reset() {
        executions.clear();
        batches = 0;
    }

    @Bean
    static BeanPostProcessor instrumentJdbc() {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String name) {
                if (!(bean instanceof DataSource source)) {
                    return bean;
                }
                return new ProbedDataSource(source);
            }
        };
    }

    private static final class ProbedDataSource extends DelegatingDataSource implements AutoCloseable {
        private final DataSource source;

        private ProbedDataSource(DataSource source) {
            super(source);
            this.source = source;
        }

        @Override
        public Connection getConnection() throws SQLException {
            return connection(super.getConnection());
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return connection(super.getConnection(username, password));
        }

        @Override
        public void close() throws Exception {
            if (source instanceof AutoCloseable closeable) {
                closeable.close();
            }
        }
    }

    private static Connection connection(Connection delegate) {
        return (Connection) Proxy.newProxyInstance(JdbcProbe.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    try {
                        Object result = method.invoke(delegate, args);
                        if (method.getName().equals("prepareStatement")) {
                            String sql = (String) args[0];
                            return Proxy.newProxyInstance(JdbcProbe.class.getClassLoader(),
                                    new Class<?>[]{PreparedStatement.class}, (p, m, a) -> {
                                        if (m.getName().equals("executeBatch")) {
                                            batches++;
                                        }
                                        if (m.getName().startsWith("execute")) {
                                            executions.add(sql);
                                        }
                                        try {
                                            return m.invoke(result, a);
                                        } catch (InvocationTargetException e) {
                                            throw e.getCause();
                                        }
                                    });
                        }
                        return result;
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }
}
