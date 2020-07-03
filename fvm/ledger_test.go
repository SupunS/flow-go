package fvm_test

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/onflow/cadence"
	"github.com/onflow/cadence/runtime"
	"github.com/stretchr/testify/require"

	"github.com/dapperlabs/flow-go/engine/execution/testutil"
	"github.com/dapperlabs/flow-go/fvm"
	"github.com/dapperlabs/flow-go/model/flow"
)

func fullKey(owner, controller, key string) string {
	// https://en.wikipedia.org/wiki/C0_and_C1_control_codes#Field_separators
	return strings.Join([]string{owner, controller, key}, "\x1F")
}

func fullKeyHash(owner, controller, key string) flow.RegisterID {
	h := sha256.New()
	_, _ = h.Write([]byte(fullKey(owner, controller, key)))
	return h.Sum(nil)
}

func Test_AccountWithNoKeys(t *testing.T) {
	chain := flow.Mainnet.Chain()

	rt := runtime.NewInterpreterRuntime()
	vm := fvm.New(rt)

	txBody := flow.NewTransactionBody().
		SetScript(createAccountScript).
		AddAuthorizer(chain.ServiceAddress())

	ctx := fvm.NewContext(
		fvm.WithChain(chain),
		fvm.WithRestrictedAccountCreation(false),
		fvm.WithTransactionProcessors([]fvm.TransactionProcessor{
			fvm.NewTransactionInvocator(),
		}),
	)

	ledger := testutil.RootBootstrappedLedger(vm, ctx)

	tx := fvm.Transaction(txBody)

	err := vm.Run(ctx, tx, ledger)
	require.NoError(t, err)
	require.NoError(t, tx.Err)

	address := flow.BytesToAddress(tx.Events[0].Fields[0].(cadence.Address).Bytes())

	require.NotPanics(t, func() {
		_, _ = vm.GetAccount(ctx, address, ledger)
	})
}

// Some old account could be created without key count register
// we recreate it in a test
func Test_AccountWithNoKeysCounter(t *testing.T) {
	chain := flow.Mainnet.Chain()

	rt := runtime.NewInterpreterRuntime()
	vm := fvm.New(rt)

	txBody := flow.NewTransactionBody().
		SetScript(createAccountScript).
		AddAuthorizer(chain.ServiceAddress())

	ctx := fvm.NewContext(
		fvm.WithChain(chain),
		fvm.WithRestrictedAccountCreation(false),
		fvm.WithTransactionProcessors([]fvm.TransactionProcessor{
			fvm.NewTransactionInvocator(),
		}),
	)

	ledger := testutil.RootBootstrappedLedger(vm, ctx)

	tx := fvm.Transaction(txBody)

	err := vm.Run(ctx, tx, ledger)
	require.NoError(t, err)
	require.NoError(t, tx.Err)

	address := flow.BytesToAddress(tx.Events[0].Fields[0].(cadence.Address).Bytes())

	countRegister := fullKeyHash(
		string(address.Bytes()),
		string(address.Bytes()),
		"public_key_count",
	)

	ledger.Delete(countRegister)

	require.NotPanics(t, func() {
		_, _ = vm.GetAccount(ctx, address, ledger)
	})
}