package bitcask2

type Option struct {
	// maxFileSize, if cur's file size exceed this, should rotate
	maxFileSize int
}

var defOpt = Option{maxFileSize: 1024 * 1024 * 2}

type OptionFunc func(opt *Option)

func WithMaxFileSz(maxFileSize int) OptionFunc {
	return func(opt *Option) {
		opt.maxFileSize = maxFileSize
	}
}

func NewOption(fn ...OptionFunc) Option {
	opt := defOpt

	for _, o := range fn {
		o(&opt)
	}

	return opt
}
