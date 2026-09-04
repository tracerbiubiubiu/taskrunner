//go:build wireinject
// +build wireinject

package app

import (
	"github.com/google/wire"

	"github.com/tracerbiubiubiu/taskrunner/internal/config"
)

// InitializeApp Wire 注入入口（生成物 wire_gen.go；生成器依赖未入 go.sum 时手工同步）。
func InitializeApp(cfg *config.Config) (*App, func(), error) {
	wire.Build(
		NewApp,
		provideLogger,
		provideStore,
		provideRedisOpt,
		provideSubmitter,
		provideInspector,
		provideTaskService,
		provideCron,
		provideAsynqServer,
		provideCallback,
		provideKeys,
		provideReadyz,
		provideEngine,
	)
	return nil, nil, nil
}
