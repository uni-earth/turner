module github.com/staaldraad/turner

go 1.20

require (
	github.com/armon/go-socks5 v0.0.0-20160902184237-e75332964ef5
	
	// 这里必须写代码里实际 import 的原始名字
	gortc.io/stun v1.22.3
	gortc.io/turn v0.11.3
	gortc.io/turnc v0.3.1
	
	golang.org/x/net v0.25.0
	
	go.uber.org/atomic v1.11.0
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.28.0
)

// 【新增核心修复】：强制重定向！告诉编译器去拉取 staaldraad 的 Fork 版本来替代原版
replace (
	gortc.io/stun => github.com/staaldraad/stun v1.22.3
	gortc.io/turn => github.com/staaldraad/turn v0.11.3
	gortc.io/turnc => github.com/staaldraad/turnc v0.3.1
)
