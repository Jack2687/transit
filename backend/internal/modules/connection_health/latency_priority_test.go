package connection_health

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"transithub/backend/internal/modules/upstream"
)

func latencyTestPolicy() Policy {
	return Policy{ID: "p1", Name: "latency", UserID: "user1", AdminAccountID: "ws1", Enabled: true, PriorityMode: PriorityModeLatency,
		AutoDegradeEnabled: true, LatencyPriority: defaultLatencyPriorityConfig(), ModelTargets: []ModelTarget{{ModelName: "model", Enabled: true}}}
}

func TestLatencyAverageUsesOnlyFreshSelectedModel(t *testing.T) {
	now := time.Now()
	p := latencyTestPolicy()
	samples := []PriorityProbeSample{
		{ID: "old", ModelName: "model", LatencyMs: 100, CreatedAt: now.Add(-301 * time.Second)},
		{ID: "fifth", ModelName: "model", LatencyMs: 2000, CreatedAt: now.Add(-240 * time.Second)},
		{ID: "fourth", ModelName: "model", LatencyMs: 3000, CreatedAt: now.Add(-180 * time.Second)},
		{ID: "third", ModelName: "model", LatencyMs: 4000, CreatedAt: now.Add(-120 * time.Second)},
		{ID: "other", ModelName: "other-model", LatencyMs: 10, CreatedAt: now},
		{ID: "latest", ModelName: "model", LatencyMs: 8000, CreatedAt: now},
		{ID: "second", ModelName: "model", LatencyMs: 5000, CreatedAt: now.Add(-60 * time.Second)},
		{ID: "future", ModelName: "model", LatencyMs: 1, CreatedAt: now.Add(time.Second)},
	}
	d := evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples, nil, now)
	if d.AverageMs == nil || math.Abs(*d.AverageMs-5100) > .001 || d.SampleCount != 5 || d.Priority != 2 {
		t.Fatalf("unexpected average: %+v", d)
	}
	p.LatencyPriority.SampleCount = 2
	p.LatencyPriority.Weights = []float64{3, 1}
	d = evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples, nil, now)
	if *d.AverageMs != 7250 || d.SampleCount != 2 {
		t.Fatalf("custom sample count/weights ignored: %+v", d)
	}
	d = evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples[:len(samples)-1], nil, now.Add(300*time.Second))
	if d.SampleCount != 1 || *d.AverageMs != 8000 {
		t.Fatalf("expiry boundary/partial weight normalization: %+v", d)
	}
	p.LatencyPriority.MinSamples = 2
	d = evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples[:len(samples)-1], nil, now.Add(300*time.Second))
	if d.Reason != "insufficient" || d.Priority != 50 {
		t.Fatalf("minimum sample threshold: %+v", d)
	}
}

func TestLatencyBandsRespectBoundaryMarginAndHealth(t *testing.T) {
	now := time.Now()
	p := latencyTestPolicy()
	p.LatencyPriority.Bands[0].Priority = 7
	p.LatencyPriority.Bands[1].Priority = 19
	for _, tt := range []struct{ ms, previous, want int }{{5000, 0, 7}, {5100, 7, 7}, {5600, 7, 19}, {4900, 19, 19}, {4400, 19, 7}, {15001, 0, 4}} {
		var previous *PrioritySyncState
		if tt.previous != 0 {
			previous = &PrioritySyncState{LastAppliedPriority: tt.previous}
		}
		d := evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, []PriorityProbeSample{{ModelName: "model", LatencyMs: tt.ms, CreatedAt: now}}, previous, now)
		if d.Priority != tt.want {
			t.Fatalf("%+v got %+v", tt, d)
		}
	}
	for _, tt := range []struct {
		state         State
		suspend, auto bool
		want          int
	}{{StateSuspended, true, true, 10000}, {StateSuspended, false, true, 100}, {StateDegraded, false, true, 100}, {StateDegraded, false, false, 7}, {StateDisabled, false, false, 10000}} {
		p.AutoSuspendEnabled = tt.suspend
		p.AutoDegradeEnabled = tt.auto
		d := evaluateLatencyPriority(upstream.PlatformSub2API, p, []ConnectionHealthState{{ModelName: "model", State: tt.state}}, []PriorityProbeSample{{ModelName: "model", LatencyMs: 2000, CreatedAt: now}}, nil, now)
		if d.Priority != tt.want {
			t.Fatalf("health override %+v got %+v", tt, d)
		}
	}
}

func TestLatencyConfigRejectsInvalidRangesAndWeights(t *testing.T) {
	for name, mutate := range map[string]func(*LatencyPriorityConfig){
		"zero samples":       func(c *LatencyPriorityConfig) { c.SampleCount = 0 },
		"weight mismatch":    func(c *LatencyPriorityConfig) { c.Weights = []float64{1} },
		"zero weight":        func(c *LatencyPriorityConfig) { c.Weights[0] = 0 },
		"nan":                func(c *LatencyPriorityConfig) { c.Weights[0] = math.NaN() },
		"expired":            func(c *LatencyPriorityConfig) { c.MaxAgeSeconds = 0 },
		"gap":                func(c *LatencyPriorityConfig) { c.Bands[1].MinSeconds = 6 },
		"overlap":            func(c *LatencyPriorityConfig) { c.Bands[1].MinSeconds = 4 },
		"empty range":        func(c *LatencyPriorityConfig) { c.Bands[1].MaxSeconds = latencyFloat(5) },
		"no final range":     func(c *LatencyPriorityConfig) { c.Bands[3].MaxSeconds = latencyFloat(20) },
		"duplicate priority": func(c *LatencyPriorityConfig) { c.Bands[1].Priority = 1 },
		"negative priority":  func(c *LatencyPriorityConfig) { c.DegradedPriority = -1 },
	} {
		t.Run(name, func(t *testing.T) {
			c := defaultLatencyPriorityConfig()
			mutate(c)
			if validateLatencyPriority(*c) {
				t.Fatal("accepted invalid config")
			}
		})
	}
	if !validateLatencyPriority(*defaultLatencyPriorityConfig()) {
		t.Fatal("default config invalid")
	}
}

func slowFailurePolicy() Policy {
	p := latencyTestPolicy()
	p.ProbeMode, p.MaxLatencyMs = ProbeModeFirstToken, 20000
	for i, priority := range []int{10, 200, 400, 600} {
		p.LatencyPriority.Bands[i].Priority = priority
	}
	return p
}

func TestLatencyTimeoutsUseWeightedBandWithSuspensionOff(t *testing.T) {
	now := time.Now()
	p := slowFailurePolicy()
	samples := []PriorityProbeSample{}
	for i := 0; i < 5; i++ {
		samples = append(samples, PriorityProbeSample{ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: string(ResultNetworkFluctuation), LatencyMs: 20000 + i*1000, CreatedAt: now.Add(-time.Duration(i) * time.Minute)})
	}
	for _, health := range []State{StateHealthy, StateDegraded, StateSuspended} {
		d := evaluateLatencyPriority(upstream.PlatformSub2API, p, []ConnectionHealthState{{ModelName: "model", State: health}}, samples, &PrioritySyncState{LastAppliedPriority: 100}, now)
		if d.AverageMs == nil || *d.AverageMs != 20000 || d.SampleCount != 5 || d.FailedSampleCount != 5 || d.Priority != 600 || d.Reason != "latency" {
			t.Fatalf("slow failures ignored or promoted with health=%s: %+v", health, d)
		}
	}
	// All failures still use the weighted result and the configured sample/band
	// rules, rather than forcing a final band that may begin above the deadline.
	p.LatencyPriority.Bands[2].MaxSeconds = latencyFloat(30)
	p.LatencyPriority.Bands[3].MinSeconds = 30
	d := evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples, nil, now)
	if d.Priority != 400 || d.Reason != "latency" {
		t.Fatalf("timeout average did not select its configured band: %+v", d)
	}
	p.LatencyPriority.MinSamples = 5
	d = evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples[:1], nil, now)
	if d.Priority != 50 || d.Reason != "insufficient" || d.SampleCount != 1 || d.FailedSampleCount != 1 || *d.AverageMs != 20000 {
		t.Fatalf("timeout samples bypassed minimum sample count: %+v", d)
	}
	p.LatencyPriority.HysteresisSeconds = 10
	d = evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples, &PrioritySyncState{LastAppliedPriority: 200}, now)
	if d.Priority != 200 {
		t.Fatalf("timeout average ignored configured band margin: %+v", d)
	}
}

func TestLatencyFailedSamplesUseConfiguredLimitWithoutChangingHistory(t *testing.T) {
	now := time.Now()
	durations := []int{20000, 25000, 60000, 5000, 21000}
	samples := make([]PriorityProbeSample, len(durations))
	for i, ms := range durations {
		result := string(ResultNetworkFluctuation)
		if i == 3 {
			result = string(ResultOK)
		}
		samples[i] = PriorityProbeSample{ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: result, LatencyMs: ms, CreatedAt: now.Add(-time.Duration(i) * time.Minute)}
	}
	for _, tt := range []struct {
		name         string
		maxLatency   int
		wantSample   int
		wantAverage  float64
		wantPriority int
	}{
		{"configured 20s", 20000, 20000, 17750, 600},
		{"configured 10s", 10000, 10000, 9250, 200},
		{"default 20s", 0, 20000, 17750, 600},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := slowFailurePolicy()
			p.MaxLatencyMs = tt.maxLatency
			d := evaluateLatencyPriority(upstream.PlatformSub2API, p, nil, samples, nil, now)
			if d.AverageMs == nil || *d.AverageMs != tt.wantAverage || d.SampleCount != 5 || d.FailedSampleCount != 4 || d.Priority != tt.wantPriority {
				t.Fatalf("failed samples were not weighted at the configured limit: %+v", d)
			}
			for i, sample := range d.Samples {
				want := tt.wantSample
				if i == 3 {
					want = 5000
				}
				if sample.LatencyMs != want || samples[i].LatencyMs != durations[i] || sample.Result != samples[i].Result {
					t.Fatalf("normalized display/history/result mismatch at %d: decision=%+v original=%+v", i, sample, samples[i])
				}
			}
		})
	}
}

func TestLatencyMixedSuccessesAndSlowFailuresRespectWindowAndDirection(t *testing.T) {
	now := time.Now()
	p := slowFailurePolicy()
	p.LatencyPriority.SampleCount, p.LatencyPriority.Weights = 2, []float64{3, 1}
	samples := []PriorityProbeSample{
		{ID: "timeout", ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: "network_fluctuation", LatencyMs: 25000, CreatedAt: now},
		{ID: "success", ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: "ok", LatencyMs: 5000, CreatedAt: now.Add(-time.Minute)},
		{ID: "quick-error", ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: "server_error", LatencyMs: 1043, CreatedAt: now},
		{ID: "other-mode", ModelName: "model", ProbeMode: ProbeModeArithmetic, Result: "network_fluctuation", LatencyMs: 90000, CreatedAt: now},
		{ID: "expired", ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: "network_fluctuation", LatencyMs: 90000, CreatedAt: now.Add(-301 * time.Second)},
		{ID: "non-probe", ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: "manual_restore", LatencyMs: 90000, CreatedAt: now},
	}
	states := []ConnectionHealthState{{ModelName: "model", State: StateDegraded}}
	d := evaluateLatencyPriority(upstream.PlatformSub2API, p, states, samples, nil, now)
	if d.AverageMs == nil || *d.AverageMs != 16250 || d.SampleCount != 2 || d.FailedSampleCount != 1 || d.Priority != 600 || d.Reason != "latency" {
		t.Fatalf("mixed sample average/health floor incorrect: %+v", d)
	}
	p.LatencyPriority.DegradedPriority = 900
	if d = evaluateLatencyPriority(upstream.PlatformSub2API, p, states, samples, nil, now); d.Priority != 900 || d.Reason != "degraded" {
		t.Fatalf("stricter health priority ignored: %+v", d)
	}
	for i, priority := range []int{600, 400, 200, 10} {
		p.LatencyPriority.Bands[i].Priority = priority
	}
	p.LatencyPriority.DegradedPriority = 100
	if d = evaluateLatencyPriority(upstream.PlatformNewAPI, p, states, samples, nil, now); d.Priority != 10 {
		t.Fatalf("NewAPI health fallback promoted a slow channel: %+v", d)
	}
	p.MaxLatencyMs = 30000
	d = evaluateLatencyPriority(upstream.PlatformNewAPI, p, nil, samples, nil, now)
	if d.SampleCount != 1 || d.FailedSampleCount != 0 || *d.AverageMs != 5000 {
		t.Fatalf("configured deadline ignored: %+v", d)
	}
}

func TestLatencyTimeoutPriorityWritesWithHealthPauseOff(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepository()
	p := slowFailurePolicy()
	p.AutoSuspendEnabled, p.AutoRemoteActionEnabled = true, false
	target := "sub2api:ws1:100"
	_ = repo.SetChannelSuspension(ctx, "user1", "ws1", target, false)
	actions := &fakeTargetPriorityActioner{}
	currentPriority := 100
	svc := &Service{repo: repo, priorityActions: actions}
	states := []ConnectionHealthState{{ConnectionID: target, ModelName: "model", State: StateSuspended}}
	for i := 0; i < 5; i++ {
		repo.events = append(repo.events, ConnectionHealthEvent{ID: string(rune('a' + i)), UserID: "user1", AdminAccountID: "ws1", ConnectionID: target, PolicyID: p.ID, ModelName: "model", ProbeMode: ProbeModeFirstToken, Result: "network_fluctuation", LatencyMs: intPtr(20000 + i*1000), CreatedAt: time.Now().Add(-time.Duration(i) * time.Minute)})
	}
	inventory := map[string]*priorityTargetInventory{target: {target: AdminProbeTarget{AccountID: "100"}, account: upstream.AdminGroupAccountInfo{Priority: &currentPriority}, policies: []Policy{p}, currentPriority: 100}}
	svc.syncWorkspacePriorities(ctx, upstream.Session{Platform: upstream.PlatformSub2API}, "user1", "ws1", inventory, true, states, nil)
	if len(actions.calls) != 1 || actions.calls[0].priority != 600 || len(repo.targetActionStates) != 0 {
		t.Fatalf("priority not changed independently of suspension: %+v", actions.calls)
	}
	stored := repo.priorityStates["user1|ws1|"+target]
	groups := []AdminGroupHealth{{Accounts: []AdminGroupAccount{{TargetID: target, AssignedPolicyIDs: []string{p.ID}}}}}
	svc.attachLatencyPriorities(ctx, "user1", "ws1", upstream.PlatformSub2API, groups, []Policy{p}, states, []PrioritySyncState{stored})
	d := groups[0].Accounts[0].LatencyPriority
	if d == nil || d.Priority != 600 || d.FailedSampleCount != 5 || d.AverageMs == nil || *d.AverageMs != 20000 {
		t.Fatalf("display disagrees with upstream decision: %+v", d)
	}
	_ = repo.SetChannelPriority(ctx, "user1", "ws1", target, false)
	currentPriority = 600
	inventory[target].currentPriority = 600
	svc.syncWorkspacePriorities(ctx, upstream.Session{Platform: upstream.PlatformSub2API}, "user1", "ws1", inventory, true, states, []PrioritySyncState{stored})
	if len(actions.calls) != 2 || actions.calls[1].priority != 100 {
		t.Fatalf("priority opt-out did not restore original: %+v", actions.calls)
	}
}

func TestLatencySharedChannelWritesOnceAndRestoresOriginalPriority(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepository()
	p := latencyTestPolicy()
	q := latencyTestPolicy()
	q.ID = "p2"
	q.LatencyPriority.Bands[0].Priority = 9
	priority := 37
	reader := fakePlatformGroupReader{groups: []upstream.AdminGroupInfo{{ID: "g1"}, {ID: "g2"}}, accountsByGrp: map[string][]upstream.AdminGroupAccountInfo{
		"g1": {{ID: "100", Priority: &priority, Models: "model"}}, "g2": {{ID: "100", Priority: &priority, Models: "model"}},
	}}
	actions := &fakeTargetPriorityActioner{}
	s := &Service{repo: repo, mySites: fakeMySitesReader{session: upstream.Session{Platform: upstream.PlatformSub2API}}, platformGroups: reader, priorityActions: actions}
	id := "sub2api:ws1:100"
	repo.events = []ConnectionHealthEvent{
		{ID: "success", UserID: "user1", AdminAccountID: "ws1", ConnectionID: id, PolicyID: p.ID, ModelName: "model", Result: "ok", LatencyMs: intPtr(3000), CreatedAt: time.Now()},
		{ID: "error", UserID: "user1", AdminAccountID: "ws1", ConnectionID: id, PolicyID: p.ID, ModelName: "model", Result: "server_error", LatencyMs: intPtr(100), CreatedAt: time.Now()},
		{ID: "manual", UserID: "user1", AdminAccountID: "ws1", ConnectionID: id, ModelName: "model", Result: "ok", LatencyMs: intPtr(19000), CreatedAt: time.Now()},
	}
	bindings := []GroupPolicyAssignment{{UserID: "user1", AdminAccountID: "ws1", AdminGroupID: "g1", PolicyID: p.ID}, {UserID: "user1", AdminAccountID: "ws1", AdminGroupID: "g2", PolicyID: q.ID}}
	s.syncMultiplierPriorities(ctx, []Policy{q, p}, nil, bindings, nil, nil)
	if len(actions.calls) != 1 || actions.calls[0].priority != 9 {
		t.Fatalf("shared channel must have one conservative result: %+v", actions.calls)
	}
	stored := repo.priorityStates["user1|ws1|"+id]
	if stored.OriginalPriority != 37 {
		t.Fatalf("original priority lost: %+v", stored)
	}
	priority = 9
	s.syncMultiplierPriorities(ctx, []Policy{p, q}, nil, bindings, nil, []PrioritySyncState{stored})
	if len(actions.calls) != 1 {
		t.Fatal("unchanged priority rewritten")
	}
	s.syncMultiplierPriorities(ctx, nil, nil, nil, nil, []PrioritySyncState{stored})
	if len(actions.calls) != 2 || actions.calls[1].priority != 37 {
		t.Fatalf("original priority not restored: %+v", actions.calls)
	}
}

func TestLatencySharedDecisionOrderAndPlatformDirection(t *testing.T) {
	p := latencyTestPolicy()
	q := latencyTestPolicy()
	q.ID = "p2"
	q.LatencyPriority.InsufficientPriority = 90
	for _, ps := range [][]Policy{{p, q}, {q, p}} {
		d := latencyDecisionForTarget(upstream.PlatformSub2API, ps, nil, nil, nil, time.Now())
		if d.Priority != 90 || d.SharedPolicyCount != 2 || d.PolicyID != "p2" {
			t.Fatalf("unstable shared decision: %+v", d)
		}
		d = latencyDecisionForTarget(upstream.PlatformNewAPI, ps, nil, nil, nil, time.Now())
		if d.Priority != 50 {
			t.Fatalf("NewAPI must prefer lower result for conservative merge: %+v", d)
		}
	}
}

type unavailableLatencySamplesRepository struct{ *fakeRepository }

func (r unavailableLatencySamplesRepository) ListPriorityProbeSamples(context.Context, string, string, []string, time.Time, ...int) ([]PriorityProbeSample, error) {
	return nil, errors.New("sample database unavailable")
}

func TestLatencySampleReadFailurePreservesUpstreamPriority(t *testing.T) {
	repo := unavailableLatencySamplesRepository{newFakeRepository()}
	p := latencyTestPolicy()
	id := "sub2api:ws1:100"
	stored := PrioritySyncState{UserID: "user1", AdminAccountID: "ws1", TargetID: id, OriginalPriority: 37, LastAppliedPriority: 2}
	repo.priorityStates["user1|ws1|"+id] = stored
	actions := &fakeTargetPriorityActioner{}
	s := &Service{repo: repo, priorityActions: actions}
	inventory := map[string]*priorityTargetInventory{id: {target: AdminProbeTarget{AccountID: "100"}, policies: []Policy{p}, currentPriority: 2}}
	s.syncWorkspacePriorities(context.Background(), upstream.Session{Platform: upstream.PlatformSub2API}, "user1", "ws1", inventory, true, nil, []PrioritySyncState{stored})
	if len(actions.calls) != 0 || repo.priorityStates["user1|ws1|"+id].LastAppliedPriority != 2 {
		t.Fatal("sample read failure changed an upstream priority or lost the checkpoint")
	}
	groups := []AdminGroupHealth{{Accounts: []AdminGroupAccount{{TargetID: id, AssignedPolicyIDs: []string{p.ID}}}}}
	s.attachLatencyPriorities(context.Background(), "user1", "ws1", upstream.PlatformSub2API, groups, []Policy{p}, nil, []PrioritySyncState{stored})
	account := groups[0].Accounts[0]
	if !account.LatencyPriorityError || account.LatencyPriority != nil {
		t.Fatal("sample read failure was displayed as a valid decision")
	}
}
