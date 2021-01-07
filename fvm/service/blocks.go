package service

import (
	"errors"
	"fmt"

	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/storage"
)

type BlockFinder struct {
	storage storage.Headers
}

func NewBlockFinder(storage storage.Headers) *BlockFinder {
	return &BlockFinder{storage: storage}
}

func (b *BlockFinder) ByHeightFrom(height uint64, header *flow.Header) (*flow.Header, error) {
	if header == nil {
		byHeight, err := b.storage.ByHeight(height)
		if err != nil {
			return nil, err
		}
		return byHeight, nil
	}

	if header.Height == height {
		return header, nil
	}

	if height > header.Height {
		return nil, fmt.Errorf("requested height (%d) larger than given header's height (%d)", height, header.Height)
	}

	id := header.ParentID

	// travel chain back
	for {
		// recent block should be in cache so this is supposed to be fast
		parent, err := b.storage.ByBlockID(id)
		if err != nil {
			return nil, fmt.Errorf("cannot retrieve block parent: %w", err)
		}
		if parent.Height == height {
			return parent, nil
		}

		_, err = b.storage.ByHeight(parent.Height)
		// if height isn't finalized, move to parent
		if err != nil && errors.Is(err, storage.ErrNotFound) {
			id = parent.ParentID
			continue
		}
		// any other error bubbles up
		if err != nil {
			return nil, fmt.Errorf("cannot retrieve block parent: %w", err)
		}
		//if parent is finalized block, we can just use finalized chain
		// to get desired height
		return b.storage.ByHeight(height)
	}
}