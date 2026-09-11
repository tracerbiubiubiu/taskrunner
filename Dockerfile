# syntax=docker/dockerfile:1
# 单容器单进程：API server + Asynq worker + cron 循环（分钟级 tick）。
# 卷挂载：/data（SQLite 部署态；PG 部署经 TASKRUNNER_DB_DRIVER=pg 无需此卷）与 /logs。
FROM golang:1.26-alpine AS build
# GOPROXY 与构建机对齐（goproxy.cn）；模块缓存挂载加速重复构建
ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -trimpath -o /out/taskrunner ./cmd/taskrunner

FROM alpine:3.21
COPY --from=build /out/taskrunner /usr/local/bin/taskrunner
# C5：固定时区（cron 语义一致性，基线 §9）；非 root 运行（卷目录预先授权）
RUN apk add --no-cache tzdata && addgroup -S app && adduser -S app -G app \
    && mkdir -p /data /logs && chown -R app:app /data /logs
USER app
ENV TZ=Asia/Shanghai
VOLUME ["/data", "/logs"]
ENV TASKRUNNER_DB_PATH=/data/taskrunner.db \
    TASKRUNNER_LOG_DIR=/logs \
    TASKRUNNER_HTTP_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["taskrunner"]
CMD ["serve"]
