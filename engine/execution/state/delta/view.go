package delta

import (
	"encoding/binary"
	"fmt"

	"github.com/onflow/flow-go/crypto/hash"
	"github.com/onflow/flow-go/fvm/state"
	"github.com/onflow/flow-go/model/flow"
)

// GetRegisterFunc is a function that returns the value for a register.
type GetRegisterFunc func(owner, controller, key string) (flow.RegisterValue, error)

// A View is a read-only view into a ledger stored in an underlying data source.
//
// A ledger view records writes to a delta that can be used to update the
// underlying data source.
type View struct {
	delta       Delta
	regTouchSet map[string]flow.RegisterID // contains all the registers that have been touched (either read or written to)
	readsCount  uint64                     // contains the total number of reads
	// SpocksSecret keeps the secret used for SPoCKs
	// TODO we can add a flag to disable capturing SpocksSecret
	// for views other than collection views to improve performance
	spockSecretHasher       hash.Hasher
	readFunc                GetRegisterFunc
	accountStorageUsedCache map[string]uint64 // cache for account storage used value
}

// Snapshot is set of interactions with the register
type Snapshot struct {
	Delta       Delta
	Reads       []flow.RegisterID
	SpockSecret []byte
}

// NewView instantiates a new ledger view with the provided read function.
func NewView(readFunc GetRegisterFunc) *View {
	return &View{
		delta:                   NewDelta(),
		regTouchSet:             make(map[string]flow.RegisterID),
		readFunc:                readFunc,
		spockSecretHasher:       hash.NewSHA3_256(),
		accountStorageUsedCache: make(map[string]uint64),
	}
}

// Snapshot returns copy of current state of interactions with a View
func (v *View) Interactions() *Snapshot {

	var delta = Delta{
		Data: make(map[string]flow.RegisterEntry, len(v.delta.Data)),
	}
	var reads = make([]flow.RegisterID, 0, len(v.regTouchSet))

	//copy data
	for s, value := range v.delta.Data {
		delta.Data[s] = value
	}

	for _, id := range v.regTouchSet {
		reads = append(reads, id)
	}

	spockSecHashSum := v.spockSecretHasher.SumHash()
	var spockSecret = make([]byte, len(spockSecHashSum))
	copy(spockSecret, spockSecHashSum)

	return &Snapshot{
		Delta:       delta,
		Reads:       reads,
		SpockSecret: spockSecret,
	}
}

// AllRegisters returns all the register IDs either in read or delta
func (r *Snapshot) AllRegisters() []flow.RegisterID {
	set := make(map[string]flow.RegisterID, len(r.Reads)+len(r.Delta.Data))
	for _, reg := range r.Reads {
		set[reg.String()] = reg
	}
	for _, reg := range r.Delta.RegisterIDs() {
		set[reg.String()] = reg
	}
	ret := make([]flow.RegisterID, 0, len(set))
	for _, r := range set {
		ret = append(ret, r)
	}
	return ret
}

// NewChild generates a new child view, with the current view as the base, sharing the Get function
func (v *View) NewChild() *View {
	return NewView(v.Get)
}

// Get gets a register value from this view.
//
// This function will return an error if it fails to read from the underlying
// data source for this view.
func (v *View) Get(owner, controller, key string) (flow.RegisterValue, error) {
	value, exists := v.delta.Get(owner, controller, key)
	if exists {
		// every time we read a value (order preserving) we update spock
		var err error = nil
		if value != nil {
			err = v.updateSpock(value)
		}
		return value, err
	}

	value, err := v.readFunc(owner, controller, key)
	if err != nil {
		return nil, err
	}

	registerID := toRegisterID(owner, controller, key)

	// capture register touch
	v.regTouchSet[registerID.String()] = registerID

	// increase reads
	v.readsCount++

	// every time we read a value (order preserving) we update spock
	err = v.updateSpock(value)
	return value, err
}

// Set sets a register value in this view.
func (v *View) Set(owner, controller, key string, value flow.RegisterValue) {
	// every time we write something to delta (order preserving) we update spock
	// TODO return the error and handle it properly on other places
	err := v.updateSpock(value)
	if err != nil {
		panic(err)
	}

	// capture register touch
	registerID := toRegisterID(owner, controller, key)

	// register size change update
	err = v.updateRegisterSizeChange(owner, controller, key, value)
	if err != nil {
		panic(err)
	}

	v.regTouchSet[registerID.String()] = registerID
	// add key value to delta
	v.delta.Set(owner, controller, key, value)
}

func (v *View) RegisterUpdates() ([]flow.RegisterID, []flow.RegisterValue) {
	return v.delta.RegisterUpdates()
}

func (v *View) updateSpock(value []byte) error {
	_, err := v.spockSecretHasher.Write(value)
	if err != nil {
		return fmt.Errorf("error updating spock secret data: %w", err)
	}
	return nil
}

func (v *View) updateRegisterSizeChange(owner, controller, key string, value flow.RegisterValue) error {
	if key == state.StorageUsedRegisterName {
		// Don't check size change on this register
		// Size of this register will always be 8
		// If this is the first time storage_used register was set it should be set to the size of itself (8)
		return nil
	}
	// get old value
	// len will be 0 if there is no old value
	oldValue, err := v.Get(owner, controller, key)
	if err != nil {
		return err
	}

	sizeChange := int64(len(value) - len(oldValue))
	if sizeChange == 0 {
		// register size has not changed. Nothing to do
		return nil
	}

	oldSize, err := v.getAccountStorageUsed(owner)
	if err != nil {
		return err
	}

	// two paths to avoid casting uint to int
	var newSize uint64
	if sizeChange < 0 {
		absChange := uint64(-sizeChange)
		if absChange > oldSize {
			// should never happen
			panic("account storage used would be negative")
		}
		newSize = oldSize - absChange
	} else {
		absChange := uint64(sizeChange)
		newSize = oldSize + absChange
	}

	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, newSize)
	// this will put us back in the Set method.
	// The difference will be that sizeChange will always be 0 when setting storage_used
	v.setAccountStorageUsed(owner, newSize)

	return nil
}

// getAccountStorageUsed gets account storage used. On the first call (for each owner) caches the result,
// next call uses the cache
func (v *View) getAccountStorageUsed(owner string) (uint64, error) {
	storageUsed, cached := v.accountStorageUsedCache[owner]
	if cached {
		return storageUsed, nil
	}

	oldSizeRegister, err := v.Get(owner, "", state.StorageUsedRegisterName)
	if err != nil {
		return 0, err
	}

	if len(oldSizeRegister) == 0 {
		// account was just created
		// set the storage to 8 which is the size of the storage register itself
		v.setAccountStorageUsed(owner, 8)
		storageUsed = 8
	} else {
		storageUsed = binary.LittleEndian.Uint64(oldSizeRegister)
	}

	// cache result
	v.accountStorageUsedCache[owner] = storageUsed
	return storageUsed, nil
}

// setAccountStorageUsed sets account storage used. Also updates the account storage used cache
func (v *View) setAccountStorageUsed(owner string, used uint64) {
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, used)
	v.Set(owner, "", state.StorageUsedRegisterName, buffer)

	//update cache
	v.accountStorageUsedCache[owner] = used
}

// Touch explicitly adds a register to the touched registers set.
func (v *View) Touch(owner, controller, key string) {

	k := toRegisterID(owner, controller, key)

	// capture register touch
	v.regTouchSet[k.String()] = k
	// increase reads
	v.readsCount++
}

// Delete removes a register in this view.
func (v *View) Delete(owner, controller, key string) {
	v.Set(owner, controller, key, nil)
}

// Delta returns a record of the registers that were mutated in this view.
func (v *View) Delta() Delta {
	return v.delta
}

// MergeView applies the changes from a the given view to this view.
// TODO rename this, this is not actually a merge as we can't merge
// readFunc s.
func (v *View) MergeView(child *View) {
	for _, id := range child.Interactions().RegisterTouches() {
		v.regTouchSet[id.String()] = id
	}
	// SpockSecret is order aware
	// TODO return the error and handle it properly on other places
	err := v.updateSpock(child.SpockSecret())
	if err != nil {
		panic(err)
	}
	v.delta.MergeWith(child.delta)
}

// RegisterTouches returns the register IDs touched by this view (either read or write)
func (r *Snapshot) RegisterTouches() []flow.RegisterID {
	ret := make([]flow.RegisterID, 0, len(r.Reads))
	ret = append(ret, r.Reads...)
	return ret
}

// ReadsCount returns the total number of reads performed on this view including all child views
func (v *View) ReadsCount() uint64 {
	return v.readsCount
}

// SpockSecret returns the secret value for SPoCK
func (v *View) SpockSecret() []byte {
	return v.spockSecretHasher.SumHash()
}
