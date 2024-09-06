package bitcask

type Option struct {
	MaxSingleFileSz int64

	// file number threshold for merging
	MergeThreshold int32
}

var defOpt = Option{MaxSingleFileSz: 1024 * 1024 * 1 /* default max size: 4MB */, MergeThreshold: 2}

type OptionFunc func(opt *Option)

func WithMaxFileSz(maxSingleFileSz int64) OptionFunc {
	return func(opt *Option) {
		opt.MaxSingleFileSz = maxSingleFileSz
	}
}

func WithMergeThreshold(MergeThreshold int32) OptionFunc {
	return func(opt *Option) {
		opt.MergeThreshold = MergeThreshold
	}
}

func NewOption(fn ...OptionFunc) Option {
	opt := defOpt

	for _, o := range fn {
		o(&opt)
	}

	return opt
}
