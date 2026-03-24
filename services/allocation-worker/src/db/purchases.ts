  async markVerified(
    purchaseId: string,
    input: {
      purchaseAmountSun: string;
      ownerShareSun: string;
      ambassadorSlug?: string | null;
      ambassadorWallet?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      purchaseAmountSun: input.purchaseAmountSun,
      ownerShareSun: input.ownerShareSun,
      status: "verified",
      failureReason: null
    };

    if (input.ambassadorSlug !== undefined) {
      updateInput.ambassadorSlug = input.ambassadorSlug;
    }

    if (input.ambassadorWallet !== undefined) {
      updateInput.ambassadorWallet = input.ambassadorWallet;
    }

    if (input.now !== undefined) {
      updateInput.now = input.now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markAllocated(
    purchaseId: string,
    input?: {
      ambassadorWallet?: string | null;
      now?: number;
    }
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "allocated",
      failureReason: null,
      allocatedAt: input?.now ?? Date.now()
    };

    if (input?.ambassadorWallet !== undefined) {
      updateInput.ambassadorWallet = input.ambassadorWallet;
    }

    if (input?.now !== undefined) {
      updateInput.now = input.now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markFailed(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "failed",
      failureReason: assertNonEmpty(reason, "reason")
    };

    if (now !== undefined) {
      updateInput.now = now;
    }

    return this.update(purchaseId, updateInput);
  }

  async markIgnored(
    purchaseId: string,
    reason: string,
    now?: number
  ): Promise<PurchaseRecord> {
    const updateInput: UpdatePurchaseRecordInput = {
      status: "ignored",
      failureReason: assertNonEmpty(reason, "reason")
    };

    if (now !== undefined) {
      updateInput.now = now;
    }

    return this.update(purchaseId, updateInput);
  }
