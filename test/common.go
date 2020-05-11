package test

import (
	"fmt"
	"log"
	"testing"
)

func errorHandle(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func assertEqual(t *testing.T, a interface{}, b interface{}, message string) {
	if a == b {
		return
	}
	if len(message) == 0 {
		message = fmt.Sprintf("%v != %v", a, b)
	}
	t.Fatal(message)
}
