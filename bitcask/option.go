package bitcask

type Option struct {
	MaxSingleFileSz int64 // 最大单个文件大小，当内容超过该大小时切换到新文件
}
