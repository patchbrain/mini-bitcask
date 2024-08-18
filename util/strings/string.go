package strings

import (
	"math/rand"
	"strings"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func GetCurrentStr(length int) string {
	var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

	var stringBuilder strings.Builder
	stringBuilder.Grow(length) // 设置字符串的容量以优化性能
	for i := 0; i < length; i++ {
		randomIndex := seededRand.Intn(len(charset))
		stringBuilder.WriteByte(charset[randomIndex])
	}
	return stringBuilder.String()
}
