FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal34 \
    && rm -rf /var/lib/apt/lists/*

COPY earthgrid-core /usr/local/bin/earthgrid-core
RUN chmod +x /usr/local/bin/earthgrid-core

EXPOSE 8400
RUN mkdir -p /data
VOLUME ["/data"]

ENTRYPOINT ["earthgrid-core"]
