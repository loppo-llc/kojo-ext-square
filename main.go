// kojo-ext-square wakes a kojo agent when something happens in a
// Square seller account.
//
// It polls Square's Orders, Payments and Refunds APIs (no webhook, no
// public URL), turns what changed into one short message per poll, and
// hands that to the agent through kojo's ext-chat API. It never calls a
// model itself; the agent looks into the details with the Square MCP
// server.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

// version is stamped by the Makefile.
var version = "dev"

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mode := ""
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	var err error
	switch mode {
	case "":
		err = runService(ctx, loadEnv(), log)
	case "version", "--version":
		fmt.Println(version)
	default:
		err = fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		log.Error("kojo-ext-square stopped", "err", err)
		os.Exit(1)
	}
}

func runService(ctx context.Context, env Env, log *slog.Logger) error {
	if env.APIBase == "" || env.Token == "" || env.AgentID == "" {
		return errors.New("KOJO_API_BASE, KOJO_EXT_TOKEN and KOJO_EXT_AGENT_ID are required; this binary is started by kojo")
	}
	log = log.With("agent", env.AgentID)
	cfg := env.Config
	token := strings.TrimSpace(env.Secrets[secretAccessToken])
	if token == "" {
		// Not worth a restart loop: kojo restarts the service by itself
		// once the operator saves a token.
		log.Warn("no Square access token configured; waiting. Set it in kojo: Settings → Extensions → Square → this agent")
		<-ctx.Done()
		return nil
	}
	if env.DataDir == "" {
		log.Warn("KOJO_EXT_DATA_DIR is not set; the poll position is kept in memory only")
	}
	store, err := openStore(env.DataDir)
	if store == nil {
		return fmt.Errorf("open state: %w", err)
	}
	kojo := newKojoClient(env.APIBase, env.Token, env.AgentID)
	deliverer := newDeliverer(kojo, store, log)
	sq := newSquareClient(cfg.BaseURL, token)
	poller := newPoller(cfg, sq, store, deliverer.Notify, log)

	if err != nil {
		log.Warn("state reset", "err", err)
		poller.note(kindWarning, "Square プラグインの保存状態が読めなかったため、現在時刻から監視を再開しました。直前の変更を取りこぼしている可能性があるので Square MCP で確認してください。")
	}
	for _, problem := range cfg.Problems {
		log.Warn("settings problem", "problem", problem)
	}
	if len(cfg.Problems) > 0 {
		poller.note(kindWarning, "Square プラグインの設定に問題があります: "+strings.Join(cfg.Problems, "; "))
	}
	log.Info("polling Square", "environment", cfg.Environment, "interval", cfg.PollInterval,
		"locations", strings.Join(cfg.LocationIDs, ","), "version", version)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		deliverer.Run(ctx)
	}()
	// Whatever a previous run left in the outbox goes out first.
	deliverer.Notify()
	poller.Run(ctx)
	wg.Wait()
	return nil
}
