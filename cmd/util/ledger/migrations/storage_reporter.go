package migrations

import (
	"github.com/rs/zerolog"

	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/ledger/common/utils"
	"github.com/onflow/flow-go/model/flow"
)

// iterates through registers keeping a map of register sizes
// reports on storage metrics
type StorageReporter struct {
	Log zerolog.Logger
}

func (r StorageReporter) Report(payload []ledger.Payload) error {
	r.Log.Info().Msg("Running Storage Reporter")
	storageUsed := make(map[string]uint64)
	isDapperAccount := make(map[string]bool)

	for _, p := range payload {
		id, err := keyToRegisterID(p.Key)
		if err != nil {
			return err
		}
		if len([]byte(id.Owner)) != flow.AddressLength {
			// not an address
			continue
		}
		if id.Key != "storage_used" {
			continue
		}
		u, _, err := utils.ReadUint64(p.Value)
		if err != nil {
			return err
		}
		storageUsed[id.Owner] = storageUsed[id.Owner] + u
		isDapperAccount[id.Owner] = false
	}
	r.Log.Info().Msg("Storage Used")
	for s, u := range storageUsed {
		r.Log.Info().Msgf("%s,%u,%t", s,u, isDapperAccount[s])
	}
	r.Log.Info().Msg("End Of Storage Used")

	return nil
}
