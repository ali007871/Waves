package com.wavesplatform.state2.patch

import com.wavesplatform.state2.LeaseInfo
import com.wavesplatform.state2.diffs.{ENOUGH_AMT, assertDiffAndState}
import org.scalacheck.{Gen, Shrink}
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, PropertyChecks}
import org.scalatest.{Matchers, PropSpec}
import scorex.lagonaki.mocks.TestBlock
import scorex.settings.TestFunctionalitySettings
import scorex.transaction.lease.{LeaseCancelTransaction, LeaseTransaction}
import scorex.transaction.{GenesisTransaction, TransactionGen}


class LeasePatchTest extends PropSpec with PropertyChecks with GeneratorDrivenPropertyChecks with Matchers with TransactionGen {

  private implicit def noShrink[A]: Shrink[A] = Shrink(_ => Stream.empty)

  private val allowMultipleLeaseCancelTransactionUntilTimestamp = TestFunctionalitySettings.Enabled.allowMultipleLeaseCancelTransactionUntilTimestamp

  property("LeasePatch cancels all active leases and its effects including those in the block") {
    val setupAndLeaseInResetBlock: Gen[(GenesisTransaction, GenesisTransaction, LeaseTransaction, LeaseCancelTransaction, LeaseTransaction)] = for {
      master <- accountGen
      recipient <- accountGen suchThat (_ != master)
      otherAccount <- accountGen
      otherAccount2 <- accountGen
      ts <- timestampGen
      genesis: GenesisTransaction = GenesisTransaction.create(master, ENOUGH_AMT, ts).right.get
      genesis2: GenesisTransaction = GenesisTransaction.create(otherAccount, ENOUGH_AMT, ts).right.get
      (lease, _) <- leaseAndCancelGeneratorP(master, recipient, master)
      fee2 <- smallFeeGen
      unleaseOther = LeaseCancelTransaction.create(otherAccount, lease.id, fee2, ts + 1).right.get
      (lease2, _) <- leaseAndCancelGeneratorP(master, otherAccount2, master)
    } yield (genesis, genesis2, lease, unleaseOther, lease2)

    forAll(setupAndLeaseInResetBlock, timestampGen suchThat (_ < allowMultipleLeaseCancelTransactionUntilTimestamp)) {
      case ((genesis, genesis2, lease, unleaseOther, lease2), blockTime) =>
        assertDiffAndState(Seq(TestBlock.create(blockTime, Seq(genesis, genesis2, lease, unleaseOther)),
          TestBlock(Seq.empty), TestBlock(Seq.empty), TestBlock(Seq.empty)), TestBlock(Seq(lease2))) { case (totalDiff, newState) =>
          newState.activeLeases() shouldBe empty
          newState.accountPortfolios.map(_._2.leaseInfo).foreach(_ shouldBe LeaseInfo.empty)
        }
    }
  }


}
