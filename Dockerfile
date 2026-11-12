# syntax=docker/dockerfile:1
# 单容器单进程：API server + Asynq worker + cron 循环（分钟级 tick）。
# 卷挂载：/data（SQLite 部署态；PG 部署经 TASKRUNNER_DB_DRIVER=pg 无需此卷）与 /logs。
FROM golang:1.27.4-alpine3.24@sha256:0a5f7fe796599ff487605987432e7619ae45beeb2fba4c2ce0b907879932aebf AS build
# 可在受限构建机传入 -p / GOMAXPROCS；不带入运行镜像。
ARG GOFLAGS
ARG GOMAXPROCS
# GOPROXY 与构建机对齐（goproxy.cn）；模块缓存挂载加速重复构建
ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -trimpath -o /out/taskrunner ./cmd/taskrunner

FROM alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
COPY --from=build /out/taskrunner /usr/local/bin/taskrunner
# C5：固定时区（cron 语义一致性，基线 §9）；非 root 运行（卷目录预先授权）
RUN apk add --no-cache tzdata && addgroup -S app && adduser -S app -G app \
    && mkdir -p /data /logs && chown -R app:app /data /logs
USER app
ENV TZ=Asia/Shanghai
VOLUME ["/data", "/logs"]
ENV TASKRUNNER_DB_PATH=/data/taskrunner.db \
    TASKRUNNER_LOG_DIR=/logs \
    TASKRUNNER_HTTP_ADDR=:18080
EXPOSE 18080
ENTRYPOINT ["taskrunner"]
CMD ["serve"]
