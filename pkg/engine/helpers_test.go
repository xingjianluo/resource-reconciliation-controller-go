package engine

import (
	"log"
	"os"
	"testing"

	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/applier"
	"github.com/xingjianluo/resource-reconciliation-controller-go/pkg/storage"
)

// testLog returns a logger that funnels engine decision traces into the
// test output, satisfying the requirement that unit-test logs show inputs
// and key judgement bases.
func testLog(t *testing.T) Logger {
	return log.New(&testWriter{t: t}, "  ", log.Ltime)
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", p)
	return len(p), nil
}

type fixture struct {
	dir   string
	store *storage.Store
	fake  *applier.FakeApplier
}

func newFixture(t *testing.T, apiVersion string) *fixture {
	t.Helper()
	dir, err := os.MkdirTemp("", "rrc-test-")
	if err != nil {
		t.Fatal(err)
	}
	st, err := storage.New(dir + "/state.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.Close()
		os.RemoveAll(dir)
	})
	return &fixture{dir: dir, store: st, fake: applier.NewFake(apiVersion)}
}

// newEngine builds an engine over the fixture with test-logging enabled.
func (f *fixture) newEngine(t *testing.T, force bool) *Engine {
	t.Helper()
	return New(f.store, f.fake, Options{Force: force, Log: testLog(t)})
}
