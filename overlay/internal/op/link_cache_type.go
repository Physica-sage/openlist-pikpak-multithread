package op

import "github.com/OpenListTeam/OpenList/v4/internal/model"

func linkCacheTypeKey(args model.LinkArgs) string {
	typeKey := "proxy/" + args.Type
	if args.Redirect {
		typeKey = "redirect/" + args.Type
	}
	if args.InternalTransfer {
		return "transfer/" + typeKey
	}
	return typeKey
}
