package store

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"xray-status/internal/checker"
)

func TestUDPHistoryKeepsMissingAndSeparateRateGrids(t *testing.T) {
	st := openTestStore(t)
	start := time.UnixMilli(1_800_000_000_000).Truncate(time.Minute)
	slots100 := make([]byte, 20)
	for i := 0; i < 10; i++ {
		binary.BigEndian.PutUint16(slots100[i*2:i*2+2], compactMissing)
	}
	binary.BigEndian.PutUint16(slots100[0:2], 80) // 20ms in 250us ticks.
	binary.BigEndian.PutUint16(slots100[4:6], 0xffff)
	if err := st.MergeUDPLiveBlock("n1", "r1", start.UnixMilli(), 100_000, slots100); err != nil {
		t.Fatal(err)
	}
	slots200 := make([]byte, 10)
	for i := 0; i < 5; i++ {
		binary.BigEndian.PutUint16(slots200[i*2:i*2+2], compactMissing)
	}
	binary.BigEndian.PutUint16(slots200[0:2], 120)
	if err := st.MergeUDPLiveBlock("n1", "r1", start.Add(20*time.Second).UnixMilli(), 200_000, slots200); err != nil {
		t.Fatal(err)
	}
	raw, _, err := st.UDPHistory("n1", "r1", start.UnixMilli(), start.Add(time.Minute).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 2 {
		t.Fatalf("want two independent rate grids, got %d", len(raw))
	}
	var row100 *UDPLiveBlockRow
	for i := range raw {
		if raw[i].IntervalUS == 100_000 {
			row100 = &raw[i]
		}
	}
	if row100 == nil || binary.BigEndian.Uint16(row100.Slots[2:4]) != compactMissing {
		t.Fatal("a locally missing probe must remain the missing sentinel")
	}
}

func TestUDPPerformanceHonorsLocalReceiverExclusions(t *testing.T) {
	st := openTestStore(t)
	if err := st.EnableSecrets("00000000000000000000000000000000000000000000000000000000000000aa"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PollWrite([]checker.Proxy{{StableID: "n1", Name: "N1", Online: true}, {StableID: "n2", Name: "N2", Online: true}}, PollWriteParams{Now: time.Now().Unix(), Today: "2026-08-07", PollInterval: 30}); err != nil {
		t.Fatal(err)
	}
	settings := DefaultUDPSettings()
	settings.Enabled = true
	if err := st.SetUDPSettings(settings); err != nil {
		t.Fatal(err)
	}
	r1, err := st.CreateUDPReceiver(CreateUDPReceiverInput{Name: "R1", Host: "127.0.0.1", UDPPort: 39090, LocalNodeSID: "n1"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.FinishUDPReceiverRegistration(r1.Receiver.ID, r1.BootstrapToken, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	r2, err := st.CreateUDPReceiver(CreateUDPReceiverInput{Name: "R2", Host: "127.0.0.2", UDPPort: 39090, LocalNodeSID: "n2"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.FinishUDPReceiverRegistration(r2.Receiver.ID, r2.BootstrapToken, "test", time.Now()); err != nil {
		t.Fatal(err)
	}
	estimate, err := st.UDPPerformance()
	if err != nil {
		t.Fatal(err)
	}
	if estimate.ActivePaths != 2 || estimate.ExpectedAssociations != 2 || math.Abs(estimate.ProbesPerSecond-20) > 0.001 {
		t.Fatalf("unexpected estimate: %+v", estimate)
	}
}

func TestUDPReceiverPairingVisibilityUniquenessAndMultiLocalSID(t *testing.T) {
	st := openTestStore(t)
	if err := st.EnableSecrets("00000000000000000000000000000000000000000000000000000000000000aa"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	created, err := st.CreateUDPReceiver(CreateUDPReceiverInput{
		Name: "Warsaw", Host: "127.0.0.10", UDPPort: 39090, LocalNodeSID: "sid-a sid-b  sid-a",
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := st.UDPReceivers(); err != nil || len(got) != 0 {
		t.Fatalf("pending receiver must stay out of active list: len=%d err=%v", len(got), err)
	}
	if created.Receiver.LocalNodeSID != "sid-a sid-b" {
		t.Fatalf("unexpected normalized local SIDs: %q", created.Receiver.LocalNodeSID)
	}
	if got, _ := st.UDPReceiverExclusions(created.Receiver.ID); len(got) != 2 || got[0] != "sid-a" || got[1] != "sid-b" {
		t.Fatalf("unexpected auto exclusions: %#v", got)
	}
	if _, _, err := st.FinishUDPReceiverRegistration(created.Receiver.ID, created.BootstrapToken, "test", now); err != nil {
		t.Fatal(err)
	}
	if got, err := st.UDPReceivers(); err != nil || len(got) != 1 {
		t.Fatalf("paired receiver must become active: len=%d err=%v", len(got), err)
	}
	receiver, err := st.UDPReceiver(created.Receiver.ID)
	if err != nil {
		t.Fatal(err)
	}
	receiver.Name = "Warsaw renamed"
	if err := st.UpdateUDPReceiver(receiver.ID, receiver); err != nil {
		t.Fatal(err)
	}
	if renamed, _ := st.UDPReceiver(receiver.ID); renamed.Name != "Warsaw renamed" {
		t.Fatalf("receiver rename did not persist: %q", renamed.Name)
	}
	receiver.UDPPort++
	if err := st.UpdateUDPReceiver(receiver.ID, receiver); err == nil {
		t.Fatal("active receiver UDP port change must require re-pairing")
	}
	if _, err := st.CreateUDPReceiver(CreateUDPReceiverInput{Name: "Duplicate", Host: "127.0.0.10", UDPPort: 39091}, now); err == nil {
		t.Fatal("registered receiver host must be unique")
	}
}

func TestDeleteUDPReceiverRemovesItFromActiveList(t *testing.T) {
	st := openTestStore(t)
	if err := st.EnableSecrets("00000000000000000000000000000000000000000000000000000000000000aa"); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	created, err := st.CreateUDPReceiver(CreateUDPReceiverInput{Name: "R", Host: "127.0.0.20", UDPPort: 39090}, now)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.FinishUDPReceiverRegistration(created.Receiver.ID, created.BootstrapToken, "test", now); err != nil {
		t.Fatal(err)
	}
	if err := st.SetUDPNodeSetting(UDPNodeSetting{NodeSID: "n1", Enabled: true, ReceiverMode: "selected", SelectedReceivers: []string{created.Receiver.ID}}); err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteUDPReceiver(created.Receiver.ID); err != nil {
		t.Fatal(err)
	}
	if got, err := st.UDPReceivers(); err != nil || len(got) != 0 {
		t.Fatalf("receiver was not deleted: len=%d err=%v", len(got), err)
	}
	settings, err := st.UDPNodeSettings()
	if err != nil {
		t.Fatal(err)
	}
	if got := settings["n1"].SelectedReceivers; len(got) != 0 {
		t.Fatalf("deleted receiver remained selected on node: %#v", got)
	}
}
