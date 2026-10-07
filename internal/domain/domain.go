package domain

import (
	"errors"
	"hash/fnv"
	"regexp"
	"strings"
)

var (
	ErrNotFound       = errors.New("not found")
	ErrExists         = errors.New("already exists")
	ErrInvalid        = errors.New("invalid request")
	ErrFenced         = errors.New("consumer assignment changed or lease expired")
	ErrOffset         = errors.New("offset out of range")
	ErrRecordTooLarge = errors.New("record exceeds read byte budget")
)
var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)

func ValidName(s string) bool { return nameRE.MatchString(s) }

// Topics become directory names. Use a portable lowercase identity so logical
// names cannot alias through Windows case folding, trailing dots or devices.
// Group/member names are not paths and retain the existing ValidName contract.
func ValidTopicName(s string) bool {
	if !ValidName(s) || s != strings.ToLower(s) || strings.HasSuffix(s, ".") {
		return false
	}
	base := strings.SplitN(s, ".", 2)[0]
	switch base {
	case "con", "prn", "aux", "nul":
		return false
	}
	if len(base) == 4 && (strings.HasPrefix(base, "com") || strings.HasPrefix(base, "lpt")) && base[3] >= '1' && base[3] <= '9' {
		return false
	}
	return true
}
func KeyPartition(key []byte, count int) int {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return int(h.Sum32() % uint32(count))
}

type Broker struct {
	ID      int
	Address string
}
type Partition struct {
	ID    int
	Owner Broker
}
type Topic struct {
	Name       string
	Partitions []Partition
}
type ConsumerGroup struct {
	Name, Topic string
	Generation  int64
}
type ConsumerMember struct{ ID string }
type ConsumerOffset struct {
	Next   int64
	Exists bool
}
type Assignment struct {
	Generation int64
	Partitions []Partition
	Members    []string
}
type RetryRecord struct {
	ID, Topic string
	Partition int
	Offset    int64
	Attempt   int
	Due       int64
	Error     string
}
