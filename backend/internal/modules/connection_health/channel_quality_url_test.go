package connection_health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"transithub/backend/internal/shared/authctx"
)

func TestChannelQualityURLSaveValidationAndLegacyClient(t *testing.T) {
	svc, _, health, runner := qualityTestService(t)
	h := &Handler{service: svc}
	ctx := context.Background()
	target := "sub2api:ws1:a"
	save := func(body string) ChannelModels {
		t.Helper()
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
		r.SetPathValue("id", target)
		r = r.WithContext(authctx.WithUserID(r.Context(), "user"))
		w := httptest.NewRecorder()
		h.saveChannelModels(w, r)
		var got ChannelModels
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &got) != nil {
			t.Fatalf("save failed: %d %s", w.Code, w.Body.String())
		}
		return got
	}
	got := save(`{"probeModel":"","qualityModel":"","qualityBaseUrl":" https://gateway.example/v1/ "}`)
	if got.QualityBaseURL != "https://gateway.example/v1" {
		t.Fatal("address not normalized")
	}
	before, _ := health.ListChannelModels(ctx, "user", "ws1")
	got = save(`{"probeModel":"health-model","qualityModel":""}`)
	after, _ := health.ListChannelModels(ctx, "user", "ws1")
	if got.QualityBaseURL != before[target].QualityBaseURL || after[target].QualityRevision != before[target].QualityRevision || after[target].QualityRevision == "" {
		t.Fatal("older client erased address or reset quality progress")
	}
	for _, bad := range []string{"gateway.example/v1", "ftp://gateway.example", "https:///v1", "https://u:p@gateway.example", "https://gateway.example?key=secret", "https://gateway.example?", "https://gateway.example/#", "https://gateway.example:0", "https://gateway.example:65536", "https://gateway.example:", "https://gate\nway.example", "https://gateway.example/" + strings.Repeat("x", 2048)} {
		if _, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityBaseURL: &bad}); err == nil {
			t.Fatalf("invalid URL accepted: %q", bad)
		}
	}
	afterInvalid, _ := health.ListChannelModels(ctx, "user", "ws1")
	if afterInvalid[target] != after[target] || runner.calls != 0 {
		t.Fatal("invalid settings changed state or made a probe request")
	}
	groups, err := svc.AdminGroups(ctx, "user")
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, g := range groups {
		for _, a := range g.Accounts {
			if a.TargetID == target {
				seen++
				if a.QualityBaseURL != got.QualityBaseURL {
					t.Fatal("shared group did not return the saved address")
				}
			}
		}
	}
	if seen != 2 || save(`{"probeModel":"","qualityModel":"","qualityBaseUrl":""}`).QualityBaseURL != "" {
		t.Fatal("shared groups or explicit clear failed")
	}
}

func TestChannelQualityURLAutomaticManualRevisionAndOriginalCredentials(t *testing.T) {
	svc, repo, health, runner := qualityTestService(t)
	enableManualQuality(t, svc)
	ctx := context.Background()
	target := "sub2api:ws1:a"
	base := "https://quality.example/v1"
	if _, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityBaseURL: &base}); err != nil {
		t.Fatal(err)
	}
	scope := QualityScope{UserID: "user", WorkspaceID: "ws1"}
	svc.runQualityScope(ctx, scope, make(chan struct{}, 32))
	if runner.calls != 1 {
		t.Fatal("automatic shared channel did not probe exactly once")
	}
	for _, method := range []string{"questions", "manxue_candy", "manxue_pelican"} {
		if _, err := svc.ProbeChannelQuality(ctx, "user", target, "two", method); err != nil {
			t.Fatal(err)
		}
	}
	for _, cred := range runner.credentials {
		if cred.BaseURL != base || cred.Key != "secret" {
			t.Fatal("quality URL override lost or original key changed")
		}
	}
	reader := svc.platformGroups.(fakePlatformGroupReader)
	if reader.credByAccount["a"].BaseURL != "http://unused.invalid" {
		t.Fatal("original channel credential was mutated")
	}
	before, _ := health.ListChannelModels(ctx, "user", "ws1")
	base += "/"
	if _, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityBaseURL: &base}); err != nil {
		t.Fatal(err)
	}
	after, _ := health.ListChannelModels(ctx, "user", "ws1")
	if after[target].QualityRevision != before[target].QualityRevision {
		t.Fatal("equivalent address reset progress")
	}
	// Editing the route during a request rejects the old route's result.
	runner.hook = func() {
		base = "https://new-quality.example/v1"
		if _, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityBaseURL: &base}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.ProbeChannelQuality(ctx, "user", target, "one", "questions"); err == nil {
		t.Fatal("old route in-flight result accepted")
	}
	if len(repo.history[qualityScopeKey("user", "ws1")]) != 4 {
		t.Fatal("address change erased history or accepted stale evidence")
	}
	runner.hook = nil
	svc.runQualityScope(ctx, scope, make(chan struct{}, 32))
	state := repo.states[qualityScopeKey("user", "ws1")][target]
	if state.Successes != 1 || runner.credentials[len(runner.credentials)-1].BaseURL != base {
		t.Fatal("new route failed to reset streak or become due")
	}
	base = ""
	if _, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityBaseURL: &base}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ProbeChannelQuality(ctx, "user", target, "one", "questions"); err != nil {
		t.Fatal(err)
	}
	if got := runner.credentials[len(runner.credentials)-1]; got.BaseURL != reader.credByAccount["a"].BaseURL || got.Key != reader.credByAccount["a"].Key {
		t.Fatal("clearing the override did not restore the original credential")
	}
}

func TestChannelQualityURLManxueWireRequest(t *testing.T) {
	for _, benchmark := range []string{"candy", "pelican"} {
		t.Run(benchmark, func(t *testing.T) {
			svc, _, _, _ := qualityTestService(t)
			q := enableManualQuality(t, svc)
			q.DetectionMethod, q.ManxueBenchmark = qualityMethodManxue, benchmark
			q.ReasoningEffort = "high"
			ctx := context.Background()
			if _, err := svc.SaveQualityConfiguration(ctx, "user", q); err != nil {
				t.Fatal(err)
			}
			base := "https://public.example/v1"
			if _, err := svc.SaveChannelModels(ctx, "user", "sub2api:ws1:a", ChannelModelsInput{QualityBaseURL: &base}); err != nil {
				t.Fatal(err)
			}
			posts := 0
			svc.qualityRunner = manxueTestRunner(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts++
					var body map[string]any
					if json.NewDecoder(r.Body).Decode(&body) != nil || body["base_url"] != base || body["api_key"] != "secret" || body["benchmark"] != benchmark {
						t.Error("Manxue did not receive the override and original key")
					}
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprintf(w, `{"id":"test-id","benchmark":%q,"status":"running"}`, benchmark)
					return
				}
				fmt.Fprintf(w, `{"id":"test-id","benchmark":%q,"status":"succeeded","candy":{"status":"passed"},"assessment":{"quality":"normal"}}`, benchmark)
			})
			svc.runQualityScope(ctx, QualityScope{UserID: "user", WorkspaceID: "ws1"}, make(chan struct{}, 32))
			sample, err := svc.ProbeChannelQuality(ctx, "user", "sub2api:ws1:a", "two", "manxue_"+benchmark)
			if err != nil || sample.Result != "passed" || posts != 2 {
				t.Fatalf("automatic/manual API checks failed: %+v %v calls=%d", sample, err, posts)
			}
		})
	}
}

func TestChannelQualityURLDoesNotChangeHealthProbe(t *testing.T) {
	svc, _, reader := cadenceFixture(1)
	svc.accounts = fakeAdminAccountResolver{id: "ws1"}
	svc.dispatcher = panicIfCalledRemoteActionRunner{}
	base := "https://quality.example/v1"
	ctx := context.Background()
	if _, err := svc.SaveChannelModels(ctx, "user1", "sub2api:ws1:0", ChannelModelsInput{QualityBaseURL: &base}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	svc.probeRunner.client.Transport = latencyTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if !strings.HasPrefix(r.URL.String(), reader.credByAccount["0"].BaseURL+"/") || r.Header.Get("Authorization") != "Bearer test" {
			t.Fatal("health probe used quality destination or changed key")
		}
		return nil, context.DeadlineExceeded
	})
	if _, err := svc.ManualProbeTarget(ctx, "user1", "sub2api:ws1:0", []string{"model-0"}); err != nil || calls != 1 {
		t.Fatal("health probe failed", err)
	}
}

func TestChannelQualityURLPostgresMigrationAndStaleResults(t *testing.T) {
	ctx, pool := qualityTestPool(t)
	migration, err := os.ReadFile("../../database/migrations/000035_channel_quality_base_url.sql")
	if err != nil || strings.TrimSpace(strings.ReplaceAll(string(migration), "\r\n", "\n")) != strings.TrimSpace(channelQualityBaseURLSchema) {
		t.Fatal("URL migration differs", err)
	}
	// Simulate a pre-upgrade row, then apply the migration twice.
	if _, err := pool.Exec(ctx, `ALTER TABLE connection_health_channel_models DROP COLUMN quality_base_url; INSERT INTO connection_health_channel_models(user_id,admin_account_id,target_id,quality_model,quality_revision) VALUES('u','w','target','model','original')`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := pool.Exec(ctx, channelQualityBaseURLSchema); err != nil {
			t.Fatal(err)
		}
	}
	repo := NewRepository(pool)
	loaded, err := repo.ListChannelModels(ctx, "u", "w")
	if err != nil || loaded["target"].QualityBaseURL != "" || loaded["target"].QualityRevision != "original" {
		t.Fatal("migration changed existing preferences", err)
	}
	q := defaultQualitySettings()
	q.Revision, q.Enabled = "global", true
	if err := repo.SaveQualitySettings(ctx, "u", "w", q); err != nil {
		t.Fatal(err)
	}
	item := loaded["target"]
	for _, base := range []string{"https://first.example/v1", "https://second.example/v1", ""} {
		old := channelQualitySettings(q, loaded["target"])
		item.QualityBaseURL = base
		if err := repo.SaveChannelModels(ctx, "u", "w", item); err != nil {
			t.Fatal(err)
		}
		loaded, err = NewRepository(pool).ListChannelModels(ctx, "u", "w")
		if err != nil || loaded["target"].QualityBaseURL != base || loaded["target"].QualityRevision == old.ChannelRevision {
			t.Fatal("address change not persisted or revision unchanged", err)
		}
		state := applyQualitySample(QualityState{}, old, old.Questions[0], QualitySample{ID: "stale", TargetID: "target", Model: old.Model, Answer: old.Questions[0].Answer, CreatedAt: time.Now()})
		if saved, err := repo.SaveManualQualityResult(ctx, "u", "w", old, state); err != nil || saved {
			t.Fatal("stale address evidence accepted", err)
		}
		if err := repo.SaveChannelModels(ctx, "u", "w", item); err != nil {
			t.Fatal(err)
		}
		repeated, _ := repo.ListChannelModels(ctx, "u", "w")
		if repeated["target"].QualityRevision != loaded["target"].QualityRevision {
			t.Fatal("unchanged address reset revision")
		}
	}
}
