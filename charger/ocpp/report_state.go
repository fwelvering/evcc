package ocpp

import (
	"fmt"
	"time"

	"github.com/evcc-io/evcc/core/keys"
	"github.com/evcc-io/evcc/db/settings"
)

// persistedReportState is what survives a restart: the open transaction, the last
// register value reported for it, and the events not yet confirmed upstream
type persistedReportState struct {
	Upstream      string           `json:"upstream,omitempty"`
	TransactionId *int             `json:"transactionId,omitempty"`
	LastWh        float64          `json:"lastWh"`
	LastAt        time.Time        `json:"lastAt"`
	Events        []persistedEvent `json:"events,omitempty"`
}

type persistedEvent struct {
	Kind reportEventKind `json:"kind"`
	At   time.Time       `json:"at"`
	Wh   float64         `json:"wh"`
}

// reportStateStore persists the state of each report connection
type reportStateStore interface {
	load(loadpointId int) (persistedReportState, bool)
	save(loadpointId int, st persistedReportState)
}

// reportStore is the store in use; tests replace it
var reportStore reportStateStore = reportSettingsStore{}

type reportSettingsStore struct{}

func reportStateKey(loadpointId int) string {
	return fmt.Sprintf("%s%d", keys.OcppReportState, loadpointId)
}

func (reportSettingsStore) load(loadpointId int) (persistedReportState, bool) {
	var st persistedReportState
	if err := settings.Json(reportStateKey(loadpointId), &st); err != nil {
		return st, false
	}
	return st, true
}

func (reportSettingsStore) save(loadpointId int, st persistedReportState) {
	if err := settings.SetJson(reportStateKey(loadpointId), st); err != nil {
		reportLog.ERROR.Printf("save report state %d: %v", loadpointId, err)
	}
}

// persistLocked saves the connection's state. Caller holds conn.mu.
func (conn *reportConnection) persistLocked() {
	st := persistedReportState{
		Upstream:      conn.rule.upstreamIdentity(),
		TransactionId: conn.transactionId,
		LastWh:        conn.lastWh,
		LastAt:        conn.lastAt,
	}
	for _, ev := range conn.events {
		st.Events = append(st.Events, persistedEvent{Kind: ev.kind, At: ev.at, Wh: ev.wh})
	}
	reportStore.save(conn.rule.LoadpointId, st)
}

// restoreLocked reloads the state saved before a restart. A session that was still
// open stays open for resumeWindow, so the loadpoint's next session can continue it
// instead of starting a second transaction. Caller holds conn.mu.
func (conn *reportConnection) restoreLocked() {
	st, ok := reportStore.load(conn.rule.LoadpointId)
	if !ok {
		return
	}

	// a transaction id only means something to the upstream that issued it
	if st.Upstream != "" && st.Upstream != conn.rule.upstreamIdentity() {
		return
	}

	conn.transactionId = st.TransactionId
	conn.lastWh = st.LastWh
	conn.lastAt = st.LastAt

	for _, ev := range st.Events {
		// a start that was already confirmed must not be sent again
		if ev.Kind == reportEventStart && conn.transactionId != nil {
			continue
		}
		conn.events = append(conn.events, reportEvent{kind: ev.Kind, at: ev.At, wh: ev.Wh})
	}

	if conn.transactionId != nil && !queuesStop(conn.events) || queuesOpenStart(conn.events) {
		conn.resumable = true
		conn.resumeTimer = time.AfterFunc(resumeWindow, func() { conn.enqueue(conn.endResumable) })
	}
}

func queuesStop(events []reportEvent) bool {
	for _, ev := range events {
		if ev.kind == reportEventStop {
			return true
		}
	}
	return false
}

// queuesOpenStart reports whether a start is queued that no stop follows
func queuesOpenStart(events []reportEvent) bool {
	open := false
	for _, ev := range events {
		switch ev.kind {
		case reportEventStart:
			open = true
		case reportEventStop:
			open = false
		}
	}
	return open
}

// tryResume claims the restored session for the loadpoint's next session start
func (conn *reportConnection) tryResume() bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if !conn.resumable {
		return false
	}

	conn.resumable = false
	conn.sessionActive = true
	if conn.resumeTimer != nil {
		conn.resumeTimer.Stop()
	}
	return true
}

// endResumable ends the restored session when no session came back to continue it
func (conn *reportConnection) endResumable() {
	conn.mu.Lock()
	if !conn.resumable {
		conn.mu.Unlock()
		return
	}
	conn.resumable = false
	conn.sessionActive = false
	reportLog.INFO.Printf("%s: no session resumed, ending restored transaction", conn.title)
	conn.push(reportEvent{kind: reportEventStop, at: conn.lastAt, wh: conn.lastWh})
	conn.mu.Unlock()

	conn.reconcile()
}
