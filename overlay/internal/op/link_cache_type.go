package op

import "github.com/OpenListTeam/OpenList/v4/internal/model"

func linkCacheTypeKey(args model.LinkArgs) string {
	if args.InternalTransfer {
		return "transfer/" + args.Type
	}
	return "regular/" + args.Type
}
