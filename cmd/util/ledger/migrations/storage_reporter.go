package migrations

import (
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"time"

	"github.com/onflow/flow-go/ledger"
	"github.com/onflow/flow-go/ledger/common/utils"
	"github.com/onflow/flow-go/model/flow"
)

// iterates through registers keeping a map of register sizes
// reports on storage metrics
type StorageReporter struct {
	Log zerolog.Logger
}

func filename() string {
	return fmt.Sprintf("./storage_report_%d.csv", int32(time.Now().Unix()))
}

func (r StorageReporter) Report(payload []ledger.Payload) error {
	fn := filename()
	r.Log.Info().Msgf("Running Storage Reporter. Saving output to %s.", fn)

	f, err := os.Create(fn)
	if err != nil {
		return err
	}

	defer func() {
		err = f.Close()
		if err != nil {
			panic(err)
		}
	}()

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
		record := []byte(fmt.Sprintf("%s,%d,%t", id.Owner, u, false))
		n, err := f.Write(record)
		if err != nil {
			return err
		}
		if n != len(record) {
			return fmt.Errorf("failed to write data to %s", fn)
		}
	}

	r.Log.Info().Msg("Storage Reporter Done.")

	return nil
}
