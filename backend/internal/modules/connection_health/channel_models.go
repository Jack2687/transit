package connection_health

import (
	"context"
	"net/http"
	"strings"
	"unicode"

	"transithub/backend/internal/shared/authctx"
	"transithub/backend/internal/shared/httpjson"
)

const channelModelsSchema = `CREATE TABLE IF NOT EXISTS connection_health_channel_models (
 user_id text NOT NULL,
 admin_account_id text NOT NULL,
 target_id text NOT NULL,
 probe_model text NOT NULL DEFAULT '',
 quality_model text NOT NULL DEFAULT '',
 quality_revision text NOT NULL DEFAULT '',
 PRIMARY KEY (user_id, admin_account_id, target_id)
);`

// Empty models inherit the policy/global defaults. Shared channels keep one
// preference across memberships, just like their automation switches.
type ChannelModels struct {
	TargetID        string `json:"targetId"`
	ProbeModel      string `json:"probeModel"`
	QualityModel    string `json:"qualityModel"`
	QualityRevision string `json:"-"`
}

func (r *Repository) ListChannelModels(ctx context.Context, user, workspace string) (map[string]ChannelModels, error) {
	rows, err := r.db.Query(ctx, `SELECT target_id,probe_model,quality_model,quality_revision FROM connection_health_channel_models WHERE user_id=$1 AND admin_account_id=$2`, user, workspace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]ChannelModels{}
	for rows.Next() {
		var item ChannelModels
		if err := rows.Scan(&item.TargetID, &item.ProbeModel, &item.QualityModel, &item.QualityRevision); err != nil {
			return nil, err
		}
		result[item.TargetID] = item
	}
	return result, rows.Err()
}

func (r *Repository) SaveChannelModels(ctx context.Context, user, workspace string, item ChannelModels) error {
	revision, err := newID()
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, `INSERT INTO connection_health_channel_models(user_id,admin_account_id,target_id,probe_model,quality_model,quality_revision)
 VALUES($1,$2,$3,$4,$5,CASE WHEN $5='' THEN '' ELSE $6 END)
 ON CONFLICT(user_id,admin_account_id,target_id) DO UPDATE SET probe_model=EXCLUDED.probe_model,quality_model=EXCLUDED.quality_model,
 quality_revision=CASE WHEN connection_health_channel_models.quality_model=EXCLUDED.quality_model THEN connection_health_channel_models.quality_revision ELSE $6 END`, user, workspace, item.TargetID, item.ProbeModel, item.QualityModel, revision)
	return err
}

func (s *Service) SaveChannelModels(ctx context.Context, user, target string, input ChannelModels) (ChannelModels, error) {
	input.ProbeModel, input.QualityModel = strings.TrimSpace(input.ProbeModel), strings.TrimSpace(input.QualityModel)
	for _, model := range []string{input.ProbeModel, input.QualityModel} {
		if len(model) > 200 || strings.ContainsFunc(model, unicode.IsControl) {
			return ChannelModels{}, requestError(qualityPrefix + "invalidModel")
		}
	}
	_, resolved, _, workspace, err := s.resolveManualTarget(ctx, user, target)
	if err != nil {
		return ChannelModels{}, err
	}
	// Serialize health model edits with health probes and remote actions. Quality
	// requests use revision checks so an in-flight old result cannot change state.
	release, err := s.repo.AcquireTargetLease(ctx, target)
	if err != nil {
		return ChannelModels{}, err
	}
	defer release()
	// Priority maintenance reads model settings under this workspace lease.
	// Finish any older sync before committing a new probe model.
	releasePriority, err := s.priorityWorkspaceLease(ctx, user, workspace)
	if err != nil {
		return ChannelModels{}, err
	}
	defer releasePriority()
	input.TargetID, input.QualityRevision = resolved.TargetID, ""
	return input, s.repo.SaveChannelModels(ctx, user, workspace, input)
}

func (h *Handler) saveChannelModels(w http.ResponseWriter, r *http.Request) {
	user, ok := authctx.UserID(r.Context())
	if !ok {
		httpjson.WriteError(w, http.StatusUnauthorized, "auth.errors.unauthorized")
		return
	}
	var input struct {
		ProbeModel   *string `json:"probeModel"`
		QualityModel *string `json:"qualityModel"`
	}
	if httpjson.Decode(r, &input) != nil || input.ProbeModel == nil || input.QualityModel == nil {
		httpjson.WriteError(w, http.StatusBadRequest, ErrorRequest)
		return
	}
	result, err := h.service.SaveChannelModels(r.Context(), user, r.PathValue("id"), ChannelModels{ProbeModel: *input.ProbeModel, QualityModel: *input.QualityModel})
	if err != nil {
		writeError(w, err)
		return
	}
	httpjson.Write(w, http.StatusOK, result)
}

// Replace only the model, retaining the policy's prompt, timing and action rules.
// A channel override uses one enabled model template per policy.
func channelProbePolicies(policies []Policy, model string) []Policy {
	if model == "" {
		return policies
	}
	out := append([]Policy(nil), policies...)
	for i := range out {
		if out[i].LatencyPriority != nil {
			config := *out[i].LatencyPriority
			config.ModelName = model
			out[i].LatencyPriority = &config
		}
		for _, target := range out[i].ModelTargets {
			if !target.Enabled {
				continue
			}
			target.ModelName = model
			out[i].ModelTargets = []ModelTarget{target}
			break
		}
	}
	return out
}

func channelProbeSpecs(models []string, policies []Policy, model string) []probeModelSpec {
	if model != "" {
		models = []string{model}
	}
	return candidateModelSpecs(models, channelProbePolicies(policies, model))
}

func channelQualitySettings(q QualitySettings, models ChannelModels) QualitySettings {
	if models.QualityModel != "" {
		q.Model = models.QualityModel
	}
	q.ChannelRevision = models.QualityRevision
	return q
}
