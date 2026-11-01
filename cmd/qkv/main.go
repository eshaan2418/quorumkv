// Command qkv is a command-line client.
//
//	qkv -servers 127.0.0.1:7001,127.0.0.1:7002 put greeting hello
//	qkv -servers ... get greeting
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/eshaan2418/quorumkv/client"
)

func main() {
	servers := flag.String("servers", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "comma-separated server addresses")
	timeout := flag.Duration("timeout", 5*time.Second, "overall deadline")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: qkv [-servers a,b,c] get KEY | put KEY VALUE | append KEY VALUE | delete KEY")
		flag.PrintDefaults()
	}
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		flag.Usage()
		os.Exit(2)
	}
	c := client.New(strings.Split(*servers, ","), time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	var err error
	switch {
	case args[0] == "get" && len(args) == 2:
		var v string
		var found bool
		if v, found, err = c.Get(ctx, args[1]); err == nil {
			if !found {
				fmt.Fprintln(os.Stderr, "(not found)")
				os.Exit(1)
			}
			fmt.Println(v)
		}
	case args[0] == "put" && len(args) == 3:
		err = c.Put(ctx, args[1], args[2])
	case args[0] == "append" && len(args) == 3:
		err = c.Append(ctx, args[1], args[2])
	case args[0] == "delete" && len(args) == 2:
		_, err = c.Delete(ctx, args[1])
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
