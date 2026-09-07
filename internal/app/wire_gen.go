// 应用装配（Wire 语义手工同步——生成器依赖 github.com/google/wire 未入 go.sum，
// 暂以本文件为唯一装配源；如需可再生：go get github.com/google/wire@v0.7.0 后
// 依下方「注入集合」重建 wireinject 文件并运行 wire）。
//
// 注入集合（InitializeApp）：
//	provideLogger / provideStore / provideRedisOpt / provideSubmitter /
//	provideInspector / provideTaskService / provideCron / provideAsynqServer /
//	provideCallback / provideKeys / provideReadyz / provideEngine → NewApp

//go:build !wireinject
// +build !wireinject

package app

import (
	"github.com/tracerbiubiubiu/taskrunner/internal/config"
)

func InitializeApp(cfg *config.Config) (*App, func(), error) {
	logger := provideLogger(cfg)
	redisOpt := provideRedisOpt(cfg)
	store, cleanup1, err := provideStore(cfg)
	if err != nil {
		return nil, nil, err
	}
	submitter, cleanup2, err := provideSubmitter(cfg, store, redisOpt)
	if err != nil {
		cleanup1()
		return nil, nil, err
	}
	inspector, cleanup3, err := provideInspector(redisOpt)
	if err != nil {
		cleanup2()
		cleanup1()
		return nil, nil, err
	}
	taskService := provideTaskService(cfg, store, submitter, inspector)
	cronLoop := provideCron(cfg, store, submitter, logger)
	asynqServer, cleanup4, err := provideAsynqServer(cfg, redisOpt, logger)
	if err != nil {
		cleanup3()
		cleanup2()
		cleanup1()
		return nil, nil, err
	}
	callbackClient := provideCallback(cfg)
	keys := provideKeys(cfg)
	ready, cleanup5, err := provideReadyz(cfg, store)
	if err != nil {
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup1()
		return nil, nil, err
	}
	engine := provideEngine(taskService, keys, ready, logger)
	app := NewApp(cfg, logger, engine, asynqServer, cronLoop, store, callbackClient)
	return app, func() {
		cleanup5()
		cleanup4()
		cleanup3()
		cleanup2()
		cleanup1()
	}, nil
}
