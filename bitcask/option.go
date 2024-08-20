package bitcask

type Option struct {
	MaxSingleFileSz int64
}

var defOpt = Option{MaxSingleFileSz: 1024 * 1024 * 4 /* default max size: 4MB */}

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
