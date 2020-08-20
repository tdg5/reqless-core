FROM redis:6

COPY qless.lua /data/qless.lua

COPY docker/qless-server /data/qless-server

CMD [ "bash", "/data/qless-server" ]
