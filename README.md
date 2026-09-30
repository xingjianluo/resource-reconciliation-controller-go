# 本地资源编排与调和控制器（Go）

一个纯本地运行的资源编排 / 调和引擎：对一批资源**声明（desired state）**做依赖排序、
并发安全的幂等收敛、失败重试、进程重启恢复，并对受保护资源实施明确的策略约束。
所有判定都带**可区分的原因码**与**可追踪的审计记录**。

- 纯 Go 标准库实现，无外部服务依赖；
- 资源的"真实世界"由 `Applier` 接口抽象（内置内存实现，可替换为真实后端）；
- 状态以 JSON **快照**持久化（内置文件存储，临时文件 + rename 原子落盘）。

## 1. 核心概念

| 概念 | 说明 |
| --- | --- |
| `Resource` | 资源声明：`Kind/Name`、语义化 `Version`、`DependsOn`、`Owner`、`Spec`、`ClaimKey`、`Revision`。 |
| `Applier` | 底层副作用接口：`Create / Update / Delete / Read`。 |
| `Store` | 状态持久化：`MemoryStore`（测试）与 `FileStore`（本地 JSON 快照）。 |
| `PolicyEngine` | 策略判定：非法声明、环 / 缺失 / 自依赖、越权、版本兼容、ClaimKey 冲突。 |
| `Reconciler` | 调和引擎：`Declare`（提交全量期望状态）、`RetryFailed`、`ReconcileAll`。 |
| `Loop` | 带**触发合并**的后台调和循环。 |
| `Snapshot` | 持久化快照：每个资源的当前 `ResourceState`、全局审计链、最新 `Revision`。 |

## 2. 调和语义（Reconciliation Semantics）

### 2.1 全量期望状态提交

`Declare(desired)` 接收的是**全量期望集合**：

- 提交中存在的资源 → 按依赖顺序创建 / 更新；
- 之前存在、但本次提交中消失的资源 → 按依赖**逆序**删除（先删依赖方，再删被依赖方）；
- 已存在且实况与声明一致的资源 → **跳过，不产生任何副作用**。

### 2.2 调和阶段（保证"拒绝不留半成品"）

每次 `Declare` 严格按以下阶段推进，前置阶段全部只读、通过后才会写状态 / 产生副作用：

1. **入组与去重**：空指针、批次内重复 key → 整批拒绝。
2. **整批结构校验**（`ValidateDeclarations`，纯只读）：
   - 空 Kind/Name/Owner、非法版本、负 Revision → `IllegalDeclaration`；
   - 资源依赖自身 → `SelfDependency`；重复依赖边 → `IllegalDeclaration`；
   - 依赖不存在的资源 → `MissingDependency`；
   - 依赖图成环 → `CycleDependency`（结果中带具体环路径，如 `[A B C A]`）；
   - 同一非空 `ClaimKey` 被两个资源占用 → `ClaimConflict`。
3. **逐条变更判定**：
   - `Revision` 旧于已观察版本 → `StaleRevision`（陈旧更新，不覆盖新声明）；
   - 相同 `Revision` 但内容被篡改 → `StaleRevision`；
   - 抢占其他存活资源的 `ClaimKey` → `ClaimConflict`；
   - 非 owner / 非 admin 修改受保护资源 → `Unauthorized`；
   - 跨主版本升级或降级 → `IncompatibleVersion`。
4. **删除集可行性与越权预校验**（全部副作用之前）：
   - 删除某资源但保留集合中仍有资源依赖它 → 整批 `MissingDependency`；
   - 删除受保护资源的请求者不是 owner/admin → 整批 `Unauthorized`。
5. **意图持久化**：通过校验后，先把目标与"删除墓碑"写入快照，再执行副作用。
   因此即使进程在副作用中途崩溃，重启后也能据快照续做，不会丢失意图。
6. **顺序执行**：创建 / 更新走拓扑正序，删除走拓扑逆序；逐资源持久化结果。

> 整批级错误通过 `Result.BatchRejected` 返回；单条级错误（如某条声明陈旧 / 越权）
> 放入 `Result.Rejected`，**不影响批次内其他合法资源**收敛。两类拒绝都零副作用、不改写实况。

### 2.3 幂等收敛

- 每次执行前先 `Read` 底层实况：实况与目标**内容完全一致**（版本、Owner、保护标记、
  ClaimKey、Spec、依赖）则记 `skip`，不调用 Update；
- 即使声明带了更新的 `Revision` 但内容无差异，也只刷新已观察版本号，**不产生副作用**；
- 只有实况与期望存在差异时才触发恰好一次 Create / Update；
- `Applier.Create` 对已存在资源、`Applier.Delete` 对不存在资源都按成功处理（底层也幂等）。

### 2.4 并发安全与触发合并

- `Reconciler` 所有公开方法在同一互斥锁下**串行**执行；并发提交同一声明时，
  只有第一个产生 Create，其余被判为重复 / 跳过（测试：30 goroutine 并发提交 → 仅 1 次 Create）。
- `Loop.Trigger` 实现触发合并：同一时刻只运行一个调和周期；周期进行中到达的多次触发
  最多排队一次（测试：50 次并发触发合并为约十几个周期，副作用总数不变）。

### 2.5 陈旧更新保护

- 每个资源记录 `ObservedRev`；`desired.Revision < ObservedRev` 一律拒绝；
- `Revision` 相等但内容不同（试图借同版本号塞不同内容）同样拒绝；
- 引擎全局还记录快照级 `Revision`，便于审计与排查。

## 3. 依赖与保护规则

### 3.1 依赖顺序

- 创建 / 更新：拓扑正序（被依赖者先建），同层按 key 字典序，保证输出确定可复现；
- 删除：拓扑逆序（依赖者先删）；
- 运行期依赖门控：
  - 依赖处于失败 / 未收敛 → 当前资源 `DependencyFailed`（**可重试**），不产生自身副作用；
  - 依赖已被永久拒绝 → 当前资源 `DependencyRejected`（**不可重试**）。

### 3.2 受保护资源

- `Protected=true` 的资源只有其 `Owner` 或管理员（`NewPolicyEngine(admins...)`）可改 / 可删；
- 删除操作者可由 `DeclareAs(ctx, desired, requester)` 指定；未指定时以资源 owner 身份提交。

### 3.3 版本兼容

- 版本必须为 `MAJOR.MINOR.PATCH`（允许 `v` 前缀，忽略 `-pre/+build` 后缀）；
- 升级要求**主版本相同且不允许降级**，否则 `IncompatibleVersion`。

### 3.4 ClaimKey 冲突

`ClaimKey` 用于建模互斥占用（如端口、独占设备）。同一 ClaimKey：

- 在同一批次内被两个资源声明 → 整批 `ClaimConflict`；
- 新资源抢占已存活资源的 ClaimKey → 该资源 `ClaimConflict`，不影响其他资源。

## 4. 失败恢复策略

| 失败类型 | 原因码 | 可重试 | 行为 |
| --- | --- | --- | --- |
| 底层瞬时故障（创建 / 更新 / 删除 / 读取失败） | `ApplierFailure` | 是 | 状态置 `Failed`/`Deleting`，保留目标与尝试次数，等待 `RetryFailed` / 下轮调和。 |
| 底层永久故障 | `ApplierFailure`（`Retriable=false`） | 否 | 明确标记不可重试，需人工介入。 |
| 依赖未收敛 | `DependencyFailed` | 是 | 依赖恢复后自动放行。 |
| 依赖被拒绝 | `DependencyRejected` | 否 | 不会因重试而变化。 |
| 声明 / 策略类问题 | 各 `Reason*` | 否 | 进入 `Rejected` 并保留审计；用**合法的新 Revision** 重新提交即可恢复。 |

恢复机制：

1. **意图先落盘**：通过校验后目标 / 墓碑先持久化，再做副作用；
2. **进程重启**：`NewReconciler` 启动即从 `Store.Load()` 恢复 `Snapshot`，
   `ReconcileAll` / `RetryFailed` 会续做未完成的创建、更新与删除墓碑；
3. **漂移修复**：重启后若发现底层实况（版本 / Spec）与快照目标不一致，会自动收敛回期望；
4. **原子快照**：`FileStore` 写临时文件 `fsync` 后 `rename`，崩溃不会留下半截 JSON；
5. **审计链**：每次 create/update/delete/skip/reject（含成功 / 失败、原因、细节、尝试次数、
   时间戳）都追加到快照的 `Audit` 中，重启后仍可追踪。

## 5. 目录结构

```
pkg/reconciler/
  types.go       核心类型（Resource / ResourceState / Snapshot / AuditRecord ...）
  errors.go      可区分原因码与 ReconcileError
  graph.go       拓扑排序 + 环路径检测
  policy.go      声明校验、越权、语义化版本兼容、Claim 冲突
  store.go       Store 接口、MemoryStore、原子 FileStore
  applier.go     Applier 接口、带故障注入的 MemoryApplier
  reconciler.go  调和引擎（Declare / RetryFailed / ReconcileAll）
  loop.go        触发合并的后台循环
  *_test.go      单元测试（25+ 用例）
cmd/demo/        本地端到端演示
```

## 6. 本地验证方法

```bash
# 1) 编译与静态检查
go build ./...
go vet ./...

# 2) 全部单元测试（含输入状态 / 关键判定日志）
go test ./... -v

# 3) 竞态检测下的并发测试
go test ./... -race -count=1

# 4) 端到端演示：依赖顺序、瞬时故障重试、幂等跳过、重启恢复、审计链
go run ./cmd/demo
```

测试覆盖与对应需求：

| 测试 | 覆盖点 |
| --- | --- |
| `TestTopoOrder_*` | 拓扑正 / 逆序、菱形 DAG、环检测与环路径 |
| `TestValidate_*` / `TestCheckChange_*` / `TestCompatibleVersion` | 非法声明、自依赖、缺失依赖、环、Claim 冲突、越权、版本规则 |
| `TestReconcile_DependencyOrderCreateDelete` | 乱序提交按依赖创建、按依赖逆序删除 |
| `TestReconcile_DeleteSetInfeasibleRejected` | 删除集不可行整批拒绝、零副作用 |
| `TestReconcile_IdempotentConvergence` | 重复提交跳过、无变化不写、差异恰好一次更新 |
| `TestReconcile_StaleRevisionRejected` | 旧 Revision / 同版本改内容被拒，不覆盖新声明 |
| `TestReconcile_BatchRejectNoSideEffects` | 含环批次整批拒绝，零副作用零状态写入 |
| `TestReconcile_ProtectedAndIncompatible` | 越权与版本不兼容原因可区分 |
| `TestReconcile_PartialFailureRetryAndDependencyBlock` | 瞬时失败重试、依赖门控、最终幂等 |
| `TestReconcile_ClaimConflictAcrossBatches` | 跨批次 Claim 抢占拒绝与审计 |
| `TestLoop_ConcurrentTriggersCoalesce` / `...NoDuplicateSideEffects` | 并发触发合并、并发重复声明零重复副作用（`-race`） |
| `TestRecovery_*` | 跨重启续做、快照往返、漂移修复、删除墓碑续删、失败链跨重启 |
| `TestFileStore_*` / `TestMemoryStore_*` | 原子落盘、无临时文件残留、深拷贝隔离 |

所有测试都通过 `t.Logf` 打印**输入状态**与**关键判定依据**（拓扑序、拒绝原因、
副作用计数、恢复后的实况等），用 `go test -v` 可直接查看。

## 7. 快速上手

```go
store := rcpkg.NewFileStore("state.json")            // 或 NewMemoryStore()
app := rcpkg.NewMemoryApplier()                       // 替换为真实 Applier
engine, _ := rcpkg.NewReconciler(store, app, rcpkg.NewPolicyEngine("root"))

res, err := engine.Declare(ctx, []*rcpkg.Resource{
    {Kind: "db", Name: "users", Version: "1.0.0", Owner: "team-a", Revision: 1},
    {Kind: "svc", Name: "api", Version: "2.3.0", Owner: "team-b", Revision: 1,
        DependsOn: []string{"db/users"}, ClaimKey: "port-8080"},
})
// res.Applied / res.Skipped / res.Failed / res.Rejected / res.BatchRejected

res, _ = engine.RetryFailed(ctx)                      // 失败重试
snap := engine.Snapshot()                             // 当前状态（深拷贝）
for _, a := range engine.Audit() { /* 审计链 */ }
```
