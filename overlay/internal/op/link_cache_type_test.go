package op

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestLinkCacheTypeKeySeparatesInternalTransfer(t *testing.T) {
	regular := linkCacheTypeKey(model.LinkArgs{})
	internal := linkCacheTypeKey(model.LinkArgs{InternalTransfer: true})
	if regular == internal {
		t.Fatalf("regular and internal transfer cache keys collide: %q", regular)
	}
	if regular != "proxy/" {
		t.Fatalf("regular key = %q, want %q", regular, "proxy/")
	}
	if internal != "transfer/proxy/" {
		t.Fatalf("internal key = %q, want %q", internal, "transfer/proxy/")
	}
}

func TestLinkCacheTypeKeyPreservesUpstreamModes(t *testing.T) {
	tests := []struct {
		args model.LinkArgs
		want string
	}{
		{model.LinkArgs{Type: "raw"}, "proxy/raw"},
		{model.LinkArgs{Type: "raw", Redirect: true}, "redirect/raw"},
		{model.LinkArgs{Type: "raw", InternalTransfer: true}, "transfer/proxy/raw"},
		{model.LinkArgs{Type: "raw", Redirect: true, InternalTransfer: true}, "transfer/redirect/raw"},
		{model.LinkArgs{Type: "transfer"}, "proxy/transfer"},
	}
	seen := make(map[string]bool)
	for _, test := range tests {
		got := linkCacheTypeKey(test.args)
		if got != test.want {
			t.Fatalf("key for %+v = %q, want %q", test.args, got, test.want)
		}
		if seen[got] {
			t.Fatalf("cache keys collide: %q", got)
		}
		seen[got] = true
	}
}
