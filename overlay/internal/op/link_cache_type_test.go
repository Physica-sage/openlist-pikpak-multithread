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
	if regular != "regular/" {
		t.Fatalf("regular key = %q, want %q", regular, "regular/")
	}
	if internal != "transfer/" {
		t.Fatalf("internal key = %q, want %q", internal, "transfer/")
	}
}
