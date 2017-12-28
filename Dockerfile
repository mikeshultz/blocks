FROM alpine

# Install dependencies
RUN apk update
RUN apk add linux-headers curl build-base gcc abuild binutils binutils-doc gcc-doc musl-dev libressl libressl-dev supervisor nginx libpq postgresql-dev python3 python3-dev musl-dev libffi-dev

# Installer doesn't create /run/nginx
RUN mkdir -p /run/nginx /etc/uwsgi
RUN chown nginx:nginx /var/log/nginx /run/nginx

# Copy source
COPY . /build/blocks
COPY conf/nginx.conf /etc/nginx/nginx.conf
COPY conf/supervisor.conf /etc/supervisord.conf
COPY conf/blocks.supervisor.conf /etc/supervisor/conf.d/blocks.conf
COPY conf/blocks.uwsgi.ini /etc/uwsgi/blocks.uwsgi.ini

RUN cd /build/blocks && python3 setup.py install

ARG DB_NAME=blocks
ARG DB_USER=blocks
ARG DB_PASS
ARG DB_HOST=localhost
ARG DB_PORT=5432
ARG JSONRPC_NODE="http://localhost:8545/"

ENV DB_NAME ${DB_NAME}
ENV DB_USER ${DB_USER}
ENV DB_PASS ${DB_PASS}
ENV DB_HOST ${DB_HOST}
ENV DB_PORT ${DB_PORT}
ENV JSONRPC_NODE ${JSONRPC_NODE}

EXPOSE 8080 8099
CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisord.conf"]