package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"streamforge/internal/client"
	"time"
)

func Connect(address string) (*client.Client, context.Context, context.CancelFunc, error) {
	c, e := client.Dial(address)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	return c, ctx, cancel, e
}
func Print(v interface{}) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
func Main(run func() error) {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "streamforge:", e)
		os.Exit(1)
	}
}
