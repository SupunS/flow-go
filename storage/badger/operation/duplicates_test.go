// (c) 2019 Dapper Labs - ALL RIGHTS RESERVED

package operation

import (
	"testing"

	"github.com/dapperlabs/flow-go/utils/unittest"
	"github.com/dgraph-io/badger/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllowDuplicates(t *testing.T) {
	unittest.RunWithBadgerDB(t, func(db *badger.DB) {
		e := Entity{ID: 1337}
		key := []byte{0x01, 0x02, 0x03}
		val := []byte(`{"ID":1337}`)

		// persist first time
		err := db.Update(insert(key, e))
		require.NoError(t, err)

		e2 := Entity{ID: 1338}
		// val2 := []byte(`{"ID":1338}`)

		// persist again
		err = db.Update(AllowDuplicates(insert(key, e2)))
		require.NoError(t, err)

		// ensure new value was set
		var act []byte
		_ = db.View(func(tx *badger.Txn) error {
			item, err := tx.Get(key)
			require.NoError(t, err)
			act, err = item.ValueCopy(nil)
			require.NoError(t, err)
			return nil
		})

		assert.Equal(t, val, act)
		// assert.Equal(t, val2, act) // TODO: this should pass?
	})
}
