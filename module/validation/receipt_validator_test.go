package validation

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/onflow/flow-go/engine"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/module"
	mock2 "github.com/onflow/flow-go/module/mock"
	"github.com/onflow/flow-go/utils/unittest"
)

func TestReceiptValidator(t *testing.T) {
	suite.Run(t, new(ReceiptValidationSuite))
}

type ReceiptValidationSuite struct {
	unittest.BaseChainSuite

	receiptValidator module.ReceiptValidator
	verifier         *mock2.Verifier
}

func (s *ReceiptValidationSuite) SetupTest() {
	s.SetupChain()
	s.verifier = &mock2.Verifier{}
	s.receiptValidator = NewReceiptValidator(s.State, s.IndexDB, s.ResultsDB, s.verifier)
}

func (s *ReceiptValidationSuite) TestReceiptValid() {
	executor := s.Identities[s.ExeID]
	valSubgrph := s.ValidSubgraphFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	receiptID := receipt.ID()
	s.verifier.On("Verify",
		receiptID[:],
		receipt.ExecutorSignature,
		executor.StakingPubKey).Return(true, nil).Once()

	err := s.receiptValidator.Validate(receipt)
	s.Require().NoError(err, "should successfully validate receipt")
	s.verifier.AssertExpectations(s.T())
}

func (s *ReceiptValidationSuite) TestReceiptNoIdentity() {
	valSubgrph := s.ValidSubgraphFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(unittest.IdentityFixture().NodeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid identity")
	s.Assert().True(engine.IsInvalidInputError(err))
}

func (s *ReceiptValidationSuite) TestReceiptInvalidStake() {
	valSubgrph := s.ValidSubgraphFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe() // call optional, as validator might check stake first

	// replace stake with invalid one
	s.Identities[s.ExeID].Stake = 0

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid stake")
	s.Assert().True(engine.IsInvalidInputError(err))
}

func (s *ReceiptValidationSuite) TestReceiptInvalidRole() {
	valSubgrph := s.ValidSubgraphFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe() // call optional, as validator might check stake first

	// replace identity with invalid one
	s.Identities[s.ExeID] = unittest.IdentityFixture(unittest.WithRole(flow.RoleConsensus))

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid identity")
	s.Assert().True(engine.IsInvalidInputError(err))
}

func (s *ReceiptValidationSuite) TestReceiptInvalidSignature() {
	executor := s.Identities[s.ExeID]

	valSubgrph := s.ValidSubgraphFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(executor.NodeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		executor.StakingPubKey).Return(false, nil).Once()

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid signature")
	s.Assert().True(engine.IsInvalidInputError(err))
	s.verifier.AssertExpectations(s.T())
}

func (s *ReceiptValidationSuite) TestReceiptTooFewChunks() {
	valSubgrph := s.ValidSubgraphFixture()
	chunks := valSubgrph.Result.Chunks
	valSubgrph.Result.Chunks = chunks[0 : len(chunks)-2] // drop the last chunk
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe()

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject with invalid chunks")
	s.Assert().True(engine.IsInvalidInputError(err))
}

func (s *ReceiptValidationSuite) TestReceiptChunkInvalidBlockID() {
	valSubgrph := s.ValidSubgraphFixture()
	valSubgrph.Result.Chunks[0].BlockID = unittest.IdentifierFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe()

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject with invalid chunks")
	s.Assert().True(engine.IsInvalidInputError(err))
}

func (s *ReceiptValidationSuite) TestReceiptInvalidCollectionIndex() {
	valSubgrph := s.ValidSubgraphFixture()
	valSubgrph.Result.Chunks[0].CollectionIndex = 42
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe()

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid collection index")
	s.Assert().True(engine.IsInvalidInputError(err))
}

func (s *ReceiptValidationSuite) TestReceiptNoPreviousResult() {
	valSubgrph := s.ValidSubgraphFixture()
	// invalidate prev execution result, it will result in failing to lookup
	// prev result during sub-graph check
	valSubgrph.PreviousResult = unittest.ExecutionResultFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe()

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid receipt")
}

func (s *ReceiptValidationSuite) TestReceiptInvalidPreviousResult() {
	valSubgrph := s.ValidSubgraphFixture()
	// invalidate prev execution result blockID, this should fail because
	// prev result points to wrong block
	valSubgrph.PreviousResult.BlockID = unittest.IdentifierFixture()
	receipt := unittest.ExecutionReceiptFixture(unittest.WithExecutorID(s.ExeID),
		unittest.WithResult(valSubgrph.Result))
	s.AddSubgraphFixtureToMempools(valSubgrph)

	s.verifier.On("Verify",
		mock.Anything,
		mock.Anything,
		mock.Anything).Return(true, nil).Maybe()

	err := s.receiptValidator.Validate(receipt)
	s.Require().Error(err, "should reject invalid previous result")
}
