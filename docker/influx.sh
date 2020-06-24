docker run -d -p 8086:8086 \
    --name influxdb \
    --log-driver json-file \
    --log-opt max-size=10m \
    --log-opt max-file=3 \
    -e INFLUXDB_DB=monitor \
    -e INFLUXDB_ADMIN_USER=admin -e INFLUXDB_ADMIN_PASSWORD=admin \
    -e INFLUXDB_WRITE_USER=exporter -e INFLUXDB_WRITE_USER_PASSWORD=exporter \
    -e INFLUXDB_READ_USER=grafana -e INFLUXDB_READ_USER_PASSWORD=grafana \ 
    -e INFLUXDB_HTTP_AUTH_ENABLED=true \ 
    influxdb
