package org.mccproxy.proxy;

public class MCCProxyConfig {
    private RedisConfig redis;
    private PostgresConfig postgres;

    public RedisConfig getRedis() {
        return redis;
    }

    public void setRedis(RedisConfig redis) {
        this.redis = redis;
    }

    public PostgresConfig getPostgres() {
        return postgres;
    }

    public void setPostgres(PostgresConfig postgres) {
        this.postgres = postgres;
    }


    public static class RedisConfig {
        private String host;
        private int port;


        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class PostgresConfig {
        private String url;
        private String user;
        private String password;


        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}