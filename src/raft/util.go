package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

func (ll LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR"}[ll]
}

// func DPrintf(level LogLevel, peerId int, component string, message string) {
// 	timestamp := time.Now().Format("15:04:05.000")
// 	log.Printf("[%s] [%s] [Peer %d] [%s] - %s", timestamp, level, peerId, component, message)
// }
