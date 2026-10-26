package connection_health

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"transithub/backend/internal/modules/upstream"
	"transithub/backend/internal/shared/authctx"
)

func (f *fakeRepository) ListChannelModels(_ context.Context, user, workspace string) (map[string]ChannelModels, error) {
	f.groupProbeMu.Lock()
	defer f.groupProbeMu.Unlock()
	out := map[string]ChannelModels{}
	for k, v := range f.channelModels[qualityScopeKey(user, workspace)] {
		out[k] = v
	}
	return out, nil
}
func (f *fakeRepository) SaveChannelModels(_ context.Context, user, workspace string, item ChannelModels) error {
	f.groupProbeMu.Lock()
	defer f.groupProbeMu.Unlock()
	key := qualityScopeKey(user, workspace)
	if f.channelModels == nil {
		f.channelModels = map[string]map[string]ChannelModels{}
	}
	if f.channelModels[key] == nil {
		f.channelModels[key] = map[string]ChannelModels{}
	}
	previous := f.channelModels[key][item.TargetID]
	item.QualityRevision = previous.QualityRevision
	if previous.QualityModel != item.QualityModel || previous.QualityBaseURL != item.QualityBaseURL {
		item.QualityRevision, _ = newID()
	}
	f.channelModels[key][item.TargetID] = item
	return nil
}

func TestChannelModelsValidationAndDefaultInheritance(t *testing.T) {
	svc, quality, health, _ := qualityTestService(t)
	ctx := context.Background()
	q := enableManualQuality(t, svc)
	target := "sub2api:ws1:a"
	for _, bad := range []ChannelModelsInput{{QualityModel: strings.Repeat("m", 201)}, {ProbeModel: "invalid\nmodel"}} {
		if _, err := svc.SaveChannelModels(ctx, "user", target, bad); err == nil {
			t.Fatal("invalid model accepted")
		}
	}
	for _, target := range []string{"sub2api:other:a", "sub2api:ws1:missing", "newapi:ws1:a"} {
		if _, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{ProbeModel: "probe"}); err == nil {
			t.Fatal("foreign target accepted")
		}
	}
	_, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{ProbeModel: " probe ", QualityModel: " quality "})
	if err != nil {
		t.Fatal(err)
	}
	groups, err := svc.AdminGroups(ctx, "user")
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		for _, a := range g.Accounts {
			if a.TargetID == target && (a.ProbeModel != "probe" || a.QualityModel != "quality" || a.QualityGlobalModel != q.Model) {
				t.Fatalf("shared preference not shown: %+v", a)
			}
		}
	}
	if len(quality.channels) != 0 || len(health.channelAutoProbes) != 0 {
		t.Fatal("model save changed automation switches")
	}
	if _, err = svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{}); err != nil {
		t.Fatal(err)
	}
	sample, err := svc.ProbeChannelQuality(ctx, "user", target, "one", "questions")
	if err != nil || sample.Model != q.Model {
		t.Fatalf("inheritance not restored: %+v %v", sample, err)
	}
	q.Model = "new-global"
	if _, err = svc.SaveQualityConfiguration(ctx, "user", q); err != nil {
		t.Fatal(err)
	}
	sample, err = svc.ProbeChannelQuality(ctx, "user", target, "two", "questions")
	if err != nil || sample.Model != q.Model {
		t.Fatal("default did not follow global edit", err)
	}
}

func TestChannelQualityModelAutomaticManualRevisionAndHistory(t *testing.T) {
	svc, repo, health, runner := qualityTestService(t)
	ctx := context.Background()
	q := enableManualQuality(t, svc)
	target := "sub2api:ws1:a"
	_, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityModel: "custom-quality"})
	if err != nil {
		t.Fatal(err)
	}
	svc.runQualityScope(ctx, QualityScope{UserID: "user", WorkspaceID: "ws1"}, make(chan struct{}, 32))
	if runner.calls != 1 || runner.settings[0].Model != "custom-quality" {
		t.Fatal("automatic check ignored channel model or was duplicated")
	}
	for _, method := range []string{"questions", "manxue_candy", "manxue_pelican"} {
		sample, err := svc.ProbeChannelQuality(ctx, "user", target, "two", method)
		want := "custom-quality"
		if method == "manxue_pelican" {
			want = manxuePelicanModel
		}
		if err != nil || sample.Model != want || qualityProbeModel(runner.settings[len(runner.settings)-1]) != want {
			t.Fatalf("method %s: %+v %v", method, sample, err)
		}
	}
	before := repo.states[qualityScopeKey("user", "ws1")][target]
	_, _ = svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{ProbeModel: "only-health-changed", QualityModel: "custom-quality"})
	settings, _ := health.ListChannelModels(ctx, "user", "ws1")
	if settings[target].QualityRevision != before.ChannelRevision {
		t.Fatal("health edit reset quality progress")
	}
	_, _ = svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityModel: "next-quality"})
	svc.runQualityScope(ctx, QualityScope{UserID: "user", WorkspaceID: "ws1"}, make(chan struct{}, 32))
	state := repo.states[qualityScopeKey("user", "ws1")][target]
	if state.Latest.Model != "next-quality" || state.Successes != 1 || state.Failures != 0 || state.Revision != q.Revision {
		t.Fatalf("model change retained old streak or missed immediate scheduling: %+v", state)
	}
	if len(repo.history[qualityScopeKey("user", "ws1")]) != 5 {
		t.Fatal("model change lost history")
	}
	// A model change during the request must not append old-model evidence.
	runner.hook = func() {
		_, err := svc.SaveChannelModels(ctx, "user", target, ChannelModelsInput{QualityModel: "changed-in-flight"})
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.ProbeChannelQuality(ctx, "user", target, "one", "questions"); err == nil {
		t.Fatal("in-flight old-model result accepted")
	}
	if len(repo.history[qualityScopeKey("user", "ws1")]) != 5 {
		t.Fatal("old model result changed history")
	}
}

func TestChannelProbeModelSchedulerManualAndPriority(t *testing.T) {
	ctx := context.Background()
	svc, repo, _ := cadenceFixture(1)
	svc.accounts = fakeAdminAccountResolver{id: "ws1"}
	svc.dispatcher = panicIfCalledRemoteActionRunner{}
	target := "sub2api:ws1:0"
	_, err := svc.SaveChannelModels(ctx, "user1", target, ChannelModelsInput{ProbeModel: "custom-probe"})
	if err != nil {
		t.Fatal(err)
	}
	models, err := svc.DiscoverTargetModels(ctx, "user1", target)
	if err != nil || len(models) != 1 || models[0].ID != "custom-probe" {
		t.Fatalf("manual selection: %+v %v", models, err)
	}
	jobs := svc.collectAdminProbeJobsWithGroups(ctx, repo.policies, repo.assignments, repo.groupAssignments, repo.groupExclusions)
	if len(jobs) != 1 || len(jobs[0].models) != 1 || jobs[0].models[0].modelName != "custom-probe" {
		t.Fatalf("scheduler ignored override: %+v", jobs)
	}
	calls := 0
	svc.probeRunner.client.Transport = latencyTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "custom-probe" {
			t.Fatalf("wrong request model: %v", body["model"])
		}
		return nil, context.DeadlineExceeded
	})
	out, err := svc.ManualProbeTarget(ctx, "user1", target, []string{"custom-probe"})
	if err != nil || len(out) != 1 || calls != 1 || len(repo.events) != 1 || repo.events[0].ModelName != "custom-probe" {
		t.Fatalf("manual request/history failed: %+v %v", out, err)
	}
	p := latencyTestPolicy()
	p.LatencyPriority.ModelName = "model"
	now := time.Now()
	decision := latencyDecisionForTarget(upstream.PlatformSub2API, []Policy{p}, nil, []PriorityProbeSample{{ModelName: "custom-probe", LatencyMs: 3000, CreatedAt: now}, {ModelName: "model", LatencyMs: 19000, CreatedAt: now}}, nil, now, "custom-probe")
	if decision == nil || decision.ModelName != "custom-probe" || decision.AverageMs == nil || *decision.AverageMs != 3000 {
		t.Fatalf("priority ignored override: %+v", decision)
	}
	if p.ModelTargets[0].ModelName != "model" || repo.policies[0].ModelTargets[0].ModelName != "model-0" {
		t.Fatal("shared policy was mutated")
	}
	_, _ = svc.SaveChannelModels(ctx, "user1", target, ChannelModelsInput{})
	models, err = svc.DiscoverTargetModels(ctx, "user1", target)
	if err != nil || models[0].ID != "model-0" {
		t.Fatal("policy inheritance not restored", err)
	}
}

func TestChannelModelsHandlerRequiresBothFields(t *testing.T) {
	svc, _, _, _ := qualityTestService(t)
	h := &Handler{service: svc}
	for _, body := range []string{`{}`, `{"probeModel":"x"}`, `{"qualityModel":null,"probeModel":""}`} {
		r := httptest.NewRequest(http.MethodPut, "/", strings.NewReader(body))
		r.SetPathValue("id", "sub2api:ws1:a")
		r = r.WithContext(authctx.WithUserID(r.Context(), "user"))
		w := httptest.NewRecorder()
		h.saveChannelModels(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("partial save accepted: %d", w.Code)
		}
	}
}

func TestChannelQualityModelChangeKeepsSuspensionUntilRecovery(t *testing.T) {
	f := newQualityActionFixture(t)
	for i := 0; i < f.q.FailureLimit; i++ {
		f.probe(t, "failed", true)
	}
	key := "user|ws1|" + qualityActionTarget
	if !f.health.targetActionStates[key].QualitySuspended {
		t.Fatal("fixture did not suspend")
	}
	if _, err := f.svc.SaveChannelModels(context.Background(), "user", qualityActionTarget, ChannelModelsInput{QualityModel: "replacement"}); err != nil {
		t.Fatal(err)
	}
	f.maintain()
	if !f.health.targetActionStates[key].QualitySuspended {
		t.Fatal("changing model released suspension without evidence")
	}
	f.probe(t, "passed", true)
	if !f.health.targetActionStates[key].QualitySuspended {
		t.Fatal("recovery threshold bypassed")
	}
	f.probe(t, "passed", true)
	if f.health.targetActionStates[key].QualitySuspended {
		t.Fatal("new model recovery did not release suspension")
	}
}

func TestChannelProbeOverridePreservesEveryProbeModeAndInactiveOptOut(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{ProbeModeLight, ProbeModeArithmetic, ProbeModeFirstToken, ProbeModeSub2API} {
		t.Run(mode, func(t *testing.T) {
			svc, repo, _ := cadenceFixture(1)
			svc.accounts = fakeAdminAccountResolver{id: "ws1"}
			repo.policies[0].ProbeMode = mode
			_, err := svc.SaveChannelModels(ctx, "user1", "sub2api:ws1:0", ChannelModelsInput{ProbeModel: "channel-model"})
			if err != nil {
				t.Fatal(err)
			}
			models, err := svc.DiscoverTargetModels(ctx, "user1", "sub2api:ws1:0")
			if err != nil || len(models) != 1 || models[0].ID != "channel-model" || models[0].ProbeMode != mode {
				t.Fatalf("mode lost: %+v %v", models, err)
			}
			repo.policies[0].Enabled = false
			jobs := svc.collectAdminProbeJobsWithGroups(ctx, repo.policies, repo.assignments, repo.groupAssignments, repo.groupExclusions)
			if len(jobs) != 0 {
				t.Fatal("model override enabled automatic probes")
			}
			models, err = svc.DiscoverTargetModels(ctx, "user1", "sub2api:ws1:0")
			if err != nil || len(models) != 1 || models[0].ID != "channel-model" || models[0].ProbeMode != ProbeModeLight {
				t.Fatalf("unmanaged manual model missing: %+v %v", models, err)
			}
		})
	}
}

func TestChannelModelsPostgresPersistenceIsolationAndStaleQuality(t *testing.T) {
	ctx, pool := qualityTestPool(t)
	migration, err := os.ReadFile("../../database/migrations/000034_channel_models.sql")
	if err != nil || strings.TrimSpace(strings.ReplaceAll(string(migration), "\r\n", "\n")) != strings.TrimSpace(channelModelsSchema) {
		t.Fatal("migration differs", err)
	}
	repo := NewRepository(pool)
	q := defaultQualitySettings()
	q.Revision = "global"
	q.Enabled = true
	if err := repo.SaveQualitySettings(ctx, "u", "w", q); err != nil {
		t.Fatal(err)
	}
	item := ChannelModels{TargetID: "target", ProbeModel: "health", QualityModel: "quality"}
	if err := repo.SaveChannelModels(ctx, "u", "w", item); err != nil {
		t.Fatal(err)
	}
	loaded, err := NewRepository(pool).ListChannelModels(ctx, "u", "w")
	if err != nil || loaded[item.TargetID].ProbeModel != item.ProbeModel || loaded[item.TargetID].QualityModel != item.QualityModel || loaded[item.TargetID].QualityRevision == "" {
		t.Fatal("persistence failed", err)
	}
	for _, scope := range [][2]string{{"other", "w"}, {"u", "other"}} {
		got, err := repo.ListChannelModels(ctx, scope[0], scope[1])
		if err != nil || len(got) != 0 {
			t.Fatal("scope leaked", err)
		}
	}
	q = channelQualitySettings(q, loaded[item.TargetID])
	state := applyQualitySample(QualityState{}, q, q.Questions[0], QualitySample{ID: "evidence", TargetID: item.TargetID, Model: q.Model, Answer: q.Questions[0].Answer, CreatedAt: time.Now()})
	if saved, err := repo.SaveManualQualityResult(ctx, "u", "w", q, state); err != nil || !saved {
		t.Fatal("current model rejected", err)
	}
	item.ProbeModel = "new-health"
	if err := repo.SaveChannelModels(ctx, "u", "w", item); err != nil {
		t.Fatal(err)
	}
	loaded, _ = repo.ListChannelModels(ctx, "u", "w")
	if loaded[item.TargetID].QualityRevision != q.ChannelRevision {
		t.Fatal("health edit reset quality revision")
	}
	item.QualityModel = ""
	if err := repo.SaveChannelModels(ctx, "u", "w", item); err != nil {
		t.Fatal(err)
	}
	state.Latest.ID = "old-result"
	if saved, err := repo.SaveManualQualityResult(ctx, "u", "w", q, state); err != nil || saved {
		t.Fatal("old model accepted", err)
	}
	history, err := repo.ListQualityHistory(ctx, "u", "w", []string{item.TargetID}, 60)
	if err != nil || len(history) != 1 || history[0].Model != "quality" {
		t.Fatal("model edit changed history", err)
	}
}
