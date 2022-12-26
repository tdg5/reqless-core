FROM redis:7.0.7

COPY qless.lua /data/qless.lua

COPY docker/qless-server /data/qless-server

CMD [ "bash", "/data/qless-server" ]
