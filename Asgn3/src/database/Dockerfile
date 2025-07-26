FROM postgres:16.2-alpine3.19

# Set time zone
RUN ln -s /usr/share/zoneinfo/Asia/Kolkata /etc/localtime

# Set the init scripts
COPY --chmod=777 init/ /docker-entrypoint-initdb.d/

USER postgres
