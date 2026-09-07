# syntax=docker/dockerfile:1
# 单容器单进程：API server + Asynq worker + cron 循环（分钟级 tick）。
# 卷挂载：/data（SQLite→PG 迁移前）与 /logs（应用日志，轮转文件留在宿主）。
FROM golang:1.26-alpine AS build
# GOPROXY 与构建机对齐（goproxy.cn）；模块缓存挂载加速重复构建
ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /src
COPY go.mod go.sum ./
# 过渡（utils 未发 v0.2.0）：go.mod replace ../zhuzhao-utils——构建需附加上下文，
# 且必须在 go mod download 之前就位（replace 目标 /zhuzhao-utils = WORKDIR 的上级）：
#   docker buildx build --build-context utils=../zhuzhao-utils -f Dockerfile .
# utils 发版摘除 replace 后，本行与 syntax 行可删除。
COPY --from=utils / /zhuzhao-utils
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod CGO_ENABLED=0 go build -trimpath -o /out/taskrunner ./cmd/taskrunner

FROM alpine:3.21
COPY --from=build /out/taskrunner /usr/local/bin/taskrunner
# C5：固定时区（cron 语义一致性，基线 §9）
RUN apk add --no-cache tzdata
ENV TZ=Asia/Shanghai
VOLUME ["/data", "/logs"]
ENV TASKRUNNER_DB_PATH=/data/taskrunner.db \
    TASKRUNNER_LOG_DIR=/logs \
    TASKRUNNER_HTTP_ADDR=:8080
EXPOSE 8080
ENTRYPOINT ["taskrunner"]
CMD ["serve"]
