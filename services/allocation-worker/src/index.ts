import {
  InMemoryPurchaseStore,
  PostgresPurchaseStore,
  PurchaseStore
} from "./db/purchases";
import {
  AllocationService
} from "./domain/allocation";
import {
  AttributionService,
  AttributionHashing
} from "./domain/attribution";
import {
  AttributionProcessor
} from "./app/processAttribution";
import {
  ControllerClient,
  ControllerClientConfig,
  TronControllerClient
} from "./tron/controller";
import {
  TronHashing
} from "./tron/hashing";

export interface AllocationWorkerDependencies {
  store: PurchaseStore;
  controllerClient: ControllerClient;
  hashing: AttributionHashing;
  attributionService: AttributionService;
  allocationService: AllocationService;
  processor: AttributionProcessor;
}

export interface CreateAllocationWorkerOptions {
  tronWeb: ControllerClientConfig["tronWeb"];
  controllerContractAddress?: string;
  store?: PurchaseStore;
  controllerClient?: ControllerClient;
  hashing?: AttributionHashing;
  useInMemoryStore?: boolean;
}

export function createAllocationWorker(
  options: CreateAllocationWorkerOptions
): AllocationWorkerDependencies {
  if (!options?.tronWeb) {
    throw new Error("tronWeb is required");
  }

  const store =
    options.store ??
    (options.useInMemoryStore
      ? new InMemoryPurchaseStore()
      : new PostgresPurchaseStore());

  const hashing =
    options.hashing ??
    new TronHashing();

  const controllerClient =
    options.controllerClient ??
    new TronControllerClient({
      tronWeb: options.tronWeb,
      contractAddress: options.controllerContractAddress
    });

  const attributionService = new AttributionService({
    store,
    controllerClient,
    hashing
  });

  const allocationService = new AllocationService({
    store,
    controllerClient
  });

  const processor = new AttributionProcessor({
    attributionService,
    allocationService
  });

  return {
    store,
    controllerClient,
    hashing,
    attributionService,
    allocationService,
    processor
  };
}

export { InMemoryPurchaseStore, PostgresPurchaseStore } from "./db/purchases";
export type {
  PurchaseProcessingStatus,
  PurchaseRecord,
  CreatePurchaseRecordInput,
  UpdatePurchaseRecordInput,
  PurchaseStore
} from "./db/purchases";

export { AttributionService } from "./domain/attribution";
export type {
  FrontendAttributionInput,
  VerifiedPurchaseInput,
  AttributionDecision,
  AttributionDecisionStatus,
  PrepareVerifiedPurchaseResult,
  AttributionHashing
} from "./domain/attribution";

export { AllocationService } from "./domain/allocation";
export type {
  AllocationDecision,
  AllocationDecisionStatus,
  ExecuteAllocationInput
} from "./domain/allocation";

export { AttributionProcessor } from "./app/processAttribution";
export type {
  ProcessAttributionConfig,
  ProcessFrontendAttributionResult,
  ProcessVerifiedPurchaseAndAllocateInput,
  ProcessVerifiedPurchaseAndAllocateResult
} from "./app/processAttribution";

export { TronControllerClient } from "./tron/controller";
export type {
  ControllerClient,
  ControllerClientConfig,
  ResolveAmbassadorBySlugHashResult,
  RecordVerifiedPurchaseInput,
  RecordVerifiedPurchaseResult
} from "./tron/controller";

export { TronHashing } from "./tron/hashing";
export type {
  PurchaseIdInput
} from "./tron/hashing";
