package bitcask

type Option struct {
	MaxSingleFileSz int64 // 最大单个文件大小，当内容超过该大小时切换到新文件
}

var defOpt = Option{MaxSingleFileSz: 1024 * 1024 * 4 /* 默认单个文件大小为4MB */}

type OptionFunc func(opt *Option)

func WithMaxFileSz(maxSingleFileSz int64) OptionFunc {
	return func(opt *Option) {
		opt.MaxSingleFileSz = maxSingleFileSz
	}
}

func NewOption(fn ...OptionFunc) *Option {
	opt := defOpt

	for _, o := range fn {
		o(&opt)
	}

	return &opt
}
