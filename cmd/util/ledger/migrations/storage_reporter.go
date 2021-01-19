package migrations

import (
	"bufio"
	"fmt"
	"github.com/onflow/flow-go/fvm/state"
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

	writer := bufio.NewWriter(f)
	defer func() {
		err = writer.Flush()
		if err != nil {
			panic(err)
		}
	}()

	l := newLed(payload)
	st := state.NewState(l)

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
		address := flow.BytesToAddress([]byte(id.Owner))
		u, _, err := utils.ReadUint64(p.Value)
		if err != nil {
			return err
		}
		dapper, err := r.isDapper(address, st)
		if err != nil {
			r.Log.Err(err).Msg("Cannot determine if this is a dapper account")
			return err
		}
		record := fmt.Sprintf("%s,%d,%t\n", address.Hex(), u, dapper)
		_, err = writer.WriteString(record)
		if err != nil {
			return err
		}
	}

	r.Log.Info().Msg("Storage Reporter Done.")

	return nil
}

func (r StorageReporter) isDapper(address flow.Address, st *state.State) (bool, error) {
	id := flow.RegisterID{
		Owner:      string(address.Bytes()),
		Controller: "",
		Key:        fmt.Sprintf("%s\x1F%s", "storage", "flowTokenVault"),
	}

	resource, err := st.Get(id.Owner, id.Controller, id.Key)
	if err != nil {
		return false, fmt.Errorf("could not load storage capacity resource at %s: %w", id.String(), err)
	}

	return len(resource) == 0, nil
}
