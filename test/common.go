package integration

import "bytes"

func containsNotFound(msg string) bool {
	return bytes.Contains(bytes.ToLower([]byte(msg)), []byte("not found"))
}
