# 单容器单进程（设计文档 §8）：API server + Asynq worker + Scheduler 同进程。
# 卷挂载：/data（SQLite）与 /logs（应用日志，轮转文件留在宿主）。
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/taskrunner ./cmd/taskrunner

FROM alpine:3.21
COPY --from=build /out/taskrunner /usr/local/bin/taskrunner
COPY configs /app/configs
WORKDIR /app
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
