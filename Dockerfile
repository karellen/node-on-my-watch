FROM python:3.11-slim-bookworm

COPY target/dist/nomw*/dist/*.whl /tmp

ENV DEBIAN_FRONTEND=noninteractive
WORKDIR /root

RUN pip install --no-input /tmp/*.whl && \
    pip cache purge && \
    nomw --version && \
    rm -rf /var/lib/{apt,dpkg,cache,log}/ && \
    rm -rf /tmp/*

ENTRYPOINT ["/usr/local/bin/nomw"]
