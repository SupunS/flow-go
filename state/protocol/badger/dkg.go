package badger

import (
	"fmt"

	"github.com/dapperlabs/flow-go/crypto"
	"github.com/dapperlabs/flow-go/model/epoch"
	"github.com/dapperlabs/flow-go/model/flow"
	"github.com/dapperlabs/flow-go/storage/badger/operation"
)

type DKG struct {
	snapshot *EpochSnapshot
}

func (d *DKG) Size() (uint, error) {
	if d.snapshot.err != nil {
		return 0, d.snapshot.err
	}

	// get the current epoch commit
	var commit epoch.Commit
	err := d.snapshot.state.db.View(operation.RetrieveEpochCommit(d.snapshot.counter, &commit))
	if err != nil {
		return 0, fmt.Errorf("could not get epoch commit: %w", err)
	}

	return uint(len(commit.DKGParticipants)), nil
}

func (d *DKG) GroupKey() (crypto.PublicKey, error) {
	if d.snapshot.err != nil {
		return nil, d.snapshot.err
	}

	// get the current epoch commit
	var commit epoch.Commit
	err := d.snapshot.state.db.View(operation.RetrieveEpochCommit(d.snapshot.counter, &commit))
	if err != nil {
		return nil, fmt.Errorf("could not get epoch commit: %w", err)
	}

	return commit.DKGGroupKey, nil
}

func (d *DKG) Index(nodeID flow.Identifier) (uint, error) {
	if d.snapshot.err != nil {
		return 0, d.snapshot.err
	}

	// get the current epoch commit
	var commit epoch.Commit
	err := d.snapshot.state.db.View(operation.RetrieveEpochCommit(d.snapshot.counter, &commit))
	if err != nil {
		return 0, fmt.Errorf("could not get epoch commit: %w", err)
	}

	participant, found := commit.DKGParticipants[nodeID]
	if !found {
		return 0, fmt.Errorf("could not find DKG participant data (%x)", nodeID)
	}

	return participant.Index, nil
}

func (d *DKG) KeyShare(nodeID flow.Identifier) (crypto.PublicKey, error) {
	if d.snapshot.err != nil {
		return nil, d.snapshot.err
	}

	// get the current epoch commit
	var commit epoch.Commit
	err := d.snapshot.state.db.View(operation.RetrieveEpochCommit(d.snapshot.counter, &commit))
	if err != nil {
		return nil, fmt.Errorf("could not get epoch commit: %w", err)
	}

	participant, found := commit.DKGParticipants[nodeID]
	if !found {
		return nil, fmt.Errorf("could not find DKG participant data (%x)", nodeID)
	}

	return participant.KeyShare, nil
}