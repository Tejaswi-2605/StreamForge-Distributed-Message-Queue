package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	pb "streamforge/api/gen"
	"streamforge/internal/config"
	"strings"
	"testing"
	"time"
)

type brokerProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	logs bytes.Buffer
}

func buildCommands(t *testing.T, c *cluster, names ...string) map[string]string {
	t.Helper()
	dir := t.TempDir()
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	paths := make(map[string]string)
	for _, name := range names {
		suffix := ""
		if runtime.GOOS == "windows" {
			suffix = ".exe"
		}
		path := filepath.Join(dir, name+suffix)
		cmd := exec.CommandContext(c.ctx, "go", "build", "-buildvcs=false", "-o", path, "./cmd/"+name)
		cmd.Dir = root
		hideProcess(cmd)
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("build %s: %v\n%s", name, e, out)
		}
		paths[name] = path
	}
	return paths
}

func brokerEnvironment(c config.Config) []string {
	var owners []string
	for _, b := range c.Brokers {
		owners = append(owners, b.Address)
	}
	mode := "write"
	if c.Sync {
		mode = "fsync"
	}
	values := map[string]string{
		"BROKER_ID": fmt.Sprint(c.ID), "BROKER_ADDRESS": c.Listen, "METRICS_ADDRESS": c.Metrics,
		"DATA_DIR": c.DataDir, "POSTGRES_DSN": c.PostgresDSN, "REDIS_ADDR": c.RedisAddr,
		"BROKER_LIST": strings.Join(owners, ","), "FSYNC_MODE": mode,
		"MAX_MESSAGE_SIZE": fmt.Sprint(c.MaxMessage), "SEGMENT_MAX_BYTES": fmt.Sprint(c.SegmentBytes),
		"INDEX_INTERVAL": fmt.Sprint(c.IndexInterval), "MAX_INFLIGHT_REQUESTS": fmt.Sprint(c.MaxInflight),
		"MAX_BATCH_SIZE": fmt.Sprint(c.MaxBatch), "MAX_FETCH_SIZE": fmt.Sprint(c.MaxFetch),
		"LEASE_TTL": c.LeaseTTL.String(), "MAX_RETRIES": fmt.Sprint(c.MaxRetries),
		"RETRY_BASE": c.RetryBase.String(), "RETRY_MAX": c.RetryMax.String(),
		"RETENTION_MAX_SEGMENTS": fmt.Sprint(c.RetentionSegments), "RETENTION_INTERVAL": c.RetentionInterval.String(),
	}
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := values[strings.ToUpper(key)]; !replaced {
			env = append(env, entry)
		}
	}
	for key, value := range values {
		env = append(env, key+"="+value)
	}
	return env
}

func startBrokerProcess(t *testing.T, c *cluster, binary string, conf config.Config) *brokerProcess {
	t.Helper()
	p := &brokerProcess{done: make(chan struct{})}
	p.cmd = exec.CommandContext(c.ctx, binary)
	p.cmd.Env = brokerEnvironment(conf)
	p.cmd.Stdout = &p.logs
	p.cmd.Stderr = &p.logs
	hideProcess(p.cmd)
	if e := p.cmd.Start(); e != nil {
		t.Fatal(e)
	}
	go func() { _ = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { _ = p.cmd.Process.Kill(); <-p.done })
	httpClient := &http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			t.Fatalf("broker exited during startup: %s", p.logs.String())
		default:
		}
		response, e := httpClient.Get("http://" + conf.Metrics + "/healthz")
		if e == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return p
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = p.cmd.Process.Kill()
	<-p.done
	t.Fatalf("broker health timeout: %s", p.logs.String())
	return nil
}

func useProcessBrokers(t *testing.T, c *cluster, binary string) []*brokerProcess {
	t.Helper()
	for i, server := range c.servers {
		server.Stop()
		c.servers[i] = nil
	}
	var processes []*brokerProcess
	for _, conf := range c.configs {
		processes = append(processes, startBrokerProcess(t, c, binary, conf))
	}
	return processes
}

func TestAbruptBrokerProcessRecovery(t *testing.T) {
	c := newCluster(t, 1)
	commands := buildCommands(t, c, "broker")
	processes := useProcessBrokers(t, c, commands["broker"])
	topic := c.topic(t, 3)
	for i := 0; i < 30; i++ {
		p := int32(i % 3)
		m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte(fmt.Sprintf("payload-%d", i))})
		if e != nil || m.Offset != int64(i/3) {
			t.Fatal(m, e)
		}
	}
	member := &pb.MemberRequest{Topic: topic, Group: c.suffix, Member: "before-crash"}
	a, e := c.client.Bootstrap.JoinConsumerGroup(c.ctx, member)
	if e != nil {
		t.Fatal(e)
	}
	for p := int32(0); p < 3; p++ {
		if e := c.client.Commit(c.ctx, &pb.CommitRequest{Topic: topic, Partition: p, Group: member.Group, Member: member.Member, Generation: a.Generation, NextOffset: 5}); e != nil {
			t.Fatal(e)
		}
	}
	// Process.Kill bypasses signal handling, GracefulStop and log Close.
	if e := processes[0].cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	<-processes[0].done
	startBrokerProcess(t, c, commands["broker"], c.configs[0])
	a, e = c.client.Bootstrap.JoinConsumerGroup(c.ctx, member)
	if e != nil {
		t.Fatal(e)
	}
	for p := int32(0); p < 3; p++ {
		f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Limit: 100})
		if e != nil || len(f.Messages) != 10 {
			t.Fatal("acknowledged messages missing after kill", p, e)
		}
		for offset, message := range f.Messages {
			if message.Offset != int64(offset) || string(message.Payload) != fmt.Sprintf("payload-%d", offset*3+int(p)) {
				t.Fatal("process-crash payload/offset mismatch")
			}
		}
		resume, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic, Partition: p, Limit: 100, Group: member.Group, Member: member.Member, Generation: a.Generation, Start: pb.StartPosition_COMMITTED})
		if e != nil || len(resume.Messages) != 5 || resume.Messages[0].Offset != 5 {
			t.Fatal("durable commit did not survive process kill", e)
		}
		m, e := c.client.Publish(c.ctx, &pb.PublishRequest{Topic: topic, Partition: &p, Payload: []byte("after-crash")})
		if e != nil || m.Offset != 10 {
			t.Fatal("offset allocation reset after process kill", m, e)
		}
	}
	t.Log("force-killed standalone broker: 30 acknowledged records recovered, commits resume at 5, new offsets continue at 10")
}

func runCLI(t *testing.T, c *cluster, binary string, args ...string) ([]byte, error) {
	t.Helper()
	cmd := exec.CommandContext(c.ctx, binary, args...)
	hideProcess(cmd)
	var output, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &stderr
	e := cmd.Run()
	if e != nil {
		return output.Bytes(), fmt.Errorf("%w: %s", e, stderr.String())
	}
	return output.Bytes(), nil
}

func decodeConsumerMessages(t *testing.T, raw []byte) []*pb.Message {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	var messages []*pb.Message
	for {
		var object map[string]json.RawMessage
		e := decoder.Decode(&object)
		if e == io.EOF {
			return messages
		}
		if e != nil {
			t.Fatalf("invalid consumer JSON: %v", e)
		}
		if _, ok := object["payload"]; ok {
			encoded, e := json.Marshal(object)
			if e != nil {
				t.Fatal(e)
			}
			message := new(pb.Message)
			if e := json.Unmarshal(encoded, message); e != nil {
				t.Fatal(e)
			}
			messages = append(messages, message)
		}
	}
}

func TestProducerConsumerCLIEndToEnd(t *testing.T) {
	c := newCluster(t, 3)
	commands := buildCommands(t, c, "broker", "admin", "producer", "consumer")
	useProcessBrokers(t, c, commands["broker"])
	address := c.configs[0].Listen
	topic := "cli." + c.suffix
	if _, e := runCLI(t, c, commands["admin"], "create-topic", "--broker", address, "--topic", topic, "--partitions", "3"); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		raw, e := runCLI(t, c, commands["producer"], "--broker", address, "--topic", topic, "--key", "customer-42", "--message", fmt.Sprintf("order-%d", i))
		if e != nil {
			t.Fatal(e)
		}
		var m pb.Message
		if e := json.Unmarshal(raw, &m); e != nil || m.Partition != 1 || m.Offset != int64(i) {
			t.Fatal("producer key/offset output", &m, e)
		}
	}
	if _, e := runCLI(t, c, commands["producer"], "--broker", address, "--topic", topic, "--partition", "2", "--message", "explicit"); e != nil {
		t.Fatal(e)
	}
	consume := func(group, from string) []*pb.Message {
		t.Helper()
		raw, e := runCLI(t, c, commands["consumer"], "--broker", address, "--topic", topic, "--group", group, "--member", "cli-reader", "--from", from)
		if e != nil {
			t.Fatal(e)
		}
		return decodeConsumerMessages(t, raw)
	}
	group := "reader." + c.suffix
	messages := consume(group, "earliest")
	if len(messages) != 4 {
		t.Fatal("consumer CLI missed published records", len(messages))
	}
	seen := make(map[string]bool)
	for _, m := range messages {
		seen[string(m.Payload)] = true
	}
	for _, payload := range []string{"order-0", "order-1", "order-2", "explicit"} {
		if !seen[payload] {
			t.Fatal("CLI payload missing", payload)
		}
	}
	if len(consume(group, "committed")) != 0 || len(consume(group+".independent", "explicit")) != 4 || len(consume(group+".latest", "latest")) != 0 {
		t.Fatal("CLI resume/independent/position behavior")
	}
	if _, e := runCLI(t, c, commands["producer"], "--broker", address, "--topic", topic, "--partition", "99", "--message", "bad"); e == nil {
		t.Fatal("invalid producer partition returned success")
	}
	for _, partition := range []string{"-2", "4294967296"} {
		if _, e := runCLI(t, c, commands["producer"], "--broker", address, "--topic", topic, "--partition", partition); e == nil {
			t.Fatal("invalid/wrapped partition accepted", partition)
		}
	}
	if _, e := runCLI(t, c, commands["consumer"], "--broker", address, "--topic", topic, "--limit", "4294967297"); e == nil {
		t.Fatal("wrapped fetch limit accepted")
	}
	if _, e := runCLI(t, c, commands["consumer"], "--broker", address, "--topic", topic, "--from", "invalid"); e == nil {
		t.Fatal("invalid consumer start returned success")
	}
	if _, e := runCLI(t, c, commands["consumer"], "--broker", address, "--topic", topic, "--group", group+".fail", "--fail"); e != nil {
		t.Fatal(e)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		count := 0
		for p := int32(0); p < 3; p++ {
			f, e := c.client.Fetch(c.ctx, &pb.FetchRequest{Topic: topic + ".retry", Partition: p, Limit: 100})
			if e != nil {
				t.Fatal(e)
			}
			count += len(f.Messages)
		}
		if count == 4 {
			t.Log("actual CLI executables on three broker processes: keyed/explicit writes, payload readback, committed resume, independent/latest reads, invalid exits and failure scheduling passed")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("consumer CLI failure mode did not dispatch four retries")
}
