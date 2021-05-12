package main

import (
	"redis-protocol-cook/internal/server"
)


func main() {
	s, err := server.NewRdsServer(6480)
	if err != nil {
		panic(err)
	}

	s.Serve()
}
