# resource-reconciliation-controller-go

一个**本地资源编排与调和控制器**：接收一组资源声明（Declaration），让本地“后端世界”
最终收敛到声明所描述的目标状态，并保证该过程在依赖关系、并发更新、失败重试和进程
重启下结果一致、幂等、可解释（每次关键判定都有日志与可追踪审计记录）。

本项目不依赖任何外部服务：后端由 `FakeApplier` 用内存世界模拟，状态由文件存储持久化，
因此所有语义都可以在本地确定性验证。

## 目录结构

```
pkg/apis/v1      声明模型、存储态、审计事件、稳定可区分的失败原因
pkg/storage      文件持久化（temp + fsync + rename 原子写），重启恢复
pkg/applier      后端接口 + 内存 FakeApplier（故障注入、副作用计数）
pkg/engine       校验、依赖拓扑波次、调和主循环、保护规则、幂等收敛
cmd/demo         端到端小场景：注入失败 → 重试收敛 → 打印审计
```

## 调和语义（Reconciliation Semantics）

调和入口：`Engine.Reconcile(ctx, generation, declarations)`。

一次提交严格按下面顺序执行，**任何结构性问题都在产生副作用之前整批拒绝，绝不留下半完成状态**：

1. **陈旧世代守卫（StaleGeneration）**
   `generation < 已接受世代` 直接拒绝，不产生任何副作用；重复提交相同世代是合法的幂等重试/恢复。
2. **整批结构校验**（见“依赖规则”）。
3. **先持久化世代，再施加副作用**：即便进程在中途崩溃，目标世代也已落盘，重启后可用同一世代幂等续跑。
4. **前向波次创建/更新**：按依赖拓扑把资源分成若干波（wave），波内无依赖关系，**并发**执行；
   上一波未全部就绪，下一波不会开始。某资源失败时，其所有下游资源在本次提交中被跳过并给出
   `ApplyFailed: dependency X did not become Ready`，不会造出孤儿资源。
5. **逆向波次删除**：声明集中消失的资源按拓扑逆序删除（先删依赖方，再删被依赖方）；
   删除失败或仍被引用的资源会保留，并继续保护其依赖不被删除。

### 幂等收敛的三道保证

- **不重复副作用**：提交由互斥锁串行化，波内有屏障；后端自身也拒绝重复 `Create`。
  并发/重复触发同一目标，每个资源最终只有 1 次 `create`（见并发与压测测试）。
- **满足态不改写**：声明经规范化（依赖排序）后做 SHA-256 摘要（`SpecHash`）。
  每轮调和都把目标哈希与**后端实况**（`Applier.Get` 读到的当前对象）的哈希对比，
  相等才判定为 `noop`，不调用后端 `Update`；判定依据不是本地记录，见下节。
- **陈旧更新不覆盖**：旧世代在入口即被拒；同一资源的更低声明版本（版本降级）也被拒。

单个资源的判定依据（后端缺失 / 后端与目标一致 / 后端与目标不一致）都会以
`[apply] CREATE|NOOP|UPDATE ... basis=...` 打印到日志，并写入审计流。

## 以后端实况为准（Live-State Reconciliation）

本地状态文件只是**加速判定与审计的缓存**，后端世界（`Applier.Get`/`Exists` 读到的内容）
才是“是否已满足”的唯一事实来源。每一轮调和、每一个资源都重新做实况判定：

- **外部漂移自愈**：运维或他人在后端把对象改得与声明不一致时，下一轮调和发现
  `backendHash != desiredHash`，执行一次 `Update` 改回声明状态；对象被外部删除时，
  下一轮发现后端缺失，执行一次 `Create` 重建。修复后再跑调和全部为 `noop`，
  不产生多余写入。不会因为“上次调和成功过”就继续当作已满足。
- **失败如实上报并持续重试**：后端 `Create`/`Update` 报错时，本轮结果计入
  `ReconcileResult.Failed`（`ApplyFailed`），本地记录持久化为 `Phase=Failed` 并写审计。
  只要后端实况还没达到声明状态，之后每一轮（隔多少轮、其他资源是否已成功都无关）
  都会重新尝试，直到某一次真正写成功——那一刻只发生一次实际写入，随后立即转为
  `noop`。不存在“没生效却报告成功、之后再也不重试”的路径。
- **重启后按实况重判**：进程重启、复用已有状态文件后，同样逐个资源对照后端实况
  判定“已满足 / 需修复”，而不是凭重启前的记录下结论。带着漂移或未生效的写入
  重启后会继续修复/重试；已满足的资源不会重复创建或更新。
- **本地记录修复**：若后端实况已与声明一致但本地记录滞后（例如崩溃发生在“写后端成功、
  落盘之前”），本轮无副作用地只做 `noop`，并把本地记录修复为 `Ready`（日志
  `REPAIR-RECORD`，写审计），不产生任何后端写入。

兼容范围：以上语义不改变原有保护规则——越权、受保护、版本不兼容、版本降级依旧被
明确拒绝；陈旧世代依旧不覆盖新状态；依赖顺序与并发无副作用的保证不变。

## 依赖规则

- 边方向：`A.dependsOn=[B]` 表示创建/更新时 **B 先于 A**，删除时 **A 先于 B**。
- 拓扑用 Kahn 算法分成波次；同波资源相互独立，可并发。
- 以下声明在施加任何副作用前被**整批拒绝**，原因彼此可区分：

| 情况 | 稳定原因 `Reason` |
|---|---|
| 空 kind/name、重名、依赖自身、保护注解取值非法 | `InvalidDeclaration` |
| `dependsOn` 指向未在本批声明中的资源 | `MissingDependency` |
| 依赖图存在环（消息中列出环路径） | `CycleDetected` |

## 保护规则（Guards）

守卫按资源判定并记录为可区分的失败（结构拒绝返回 `error`；资源级拒绝进入 `ReconcileResult.Failed`，
原对象保持不变，同时写审计）。

| 规则 | 稳定原因 `Reason` | 说明 |
|---|---|---|
| 越权修改/删除他人资源 | `ProtectedOwnership` | 已存资源 `owner != rrc-go` 时拒绝；`Force` 可放行 |
| 受保护资源内容变更/删除 | `RuleConflict` | 注解 `.../protected: "true"`；`Force` 可放行 |
| 后端 API 版本过低 | `IncompatibleVersion` | 注解 `.../min-applier-version` 与后端版本做数字段比较 |
| 声明版本降级 | `IncompatibleVersion` | 目标 `Version` 低于已存有效版本即拒绝，旧声明不得覆盖新状态 |
| 后端施加失败 / 删除失败 | `ApplyFailed` / `DeleteFailed` | 保留 `Failed` 阶段与失败条件，等待重试 |
| 存储不可用 | `StorageUnavailable` | 文件原子写失败时包装返回 |

所有拒绝都通过 append-only **审计流**（`AuditEvent`：世代、资源、动作、原因、消息、序号、时间）
落盘，可在状态文件中追踪。

## 失败恢复与重启策略

- **部分失败重试**：失败资源持久化为 `Phase=Failed` 并带失败条件；健康资源保持 `Ready`。
  重试判定以后端实况为准：健康资源后端与声明一致为 `noop`，只重试后端实况未达标的资源；
  后端故障“自愈”后即收敛。失败资源若已不在后端（崩溃/丢失/被外部删除），引擎检测后端
  真实存在性并**补建**，而不是盲目更新。
- **进程重启恢复**：存储对每次变更做原子快照（同目录临时文件 → `fsync` → `rename`），
  崩溃只会留下旧文件或新文件，不会产生半个文件。重启后重新打开同一个 store：
  - 世代恢复 → 旧世代依旧被拒；
  - 逐个资源对照后端实况重判 → 已满足的判 `noop` 不重复创建，被外部改过的修复、
    被外部删掉的重建、上次没写成功的继续重试（见上节“以后端实况为准”）。
- **删除安全性**：逆波次删除 + 引用检查；仍被（保留中的或期望中的）资源引用的依赖不会被删除。

## 本地验证方法

要求 Go 1.25+（开发使用 Go 1.26）。

```bash
# 编译
go build ./...

# 全量单测（-v 可看到每个用例打印的 INPUT 状态与 JUDGE 关键判定依据）
go test ./... -v

# 竞态检测（并发调和场景）
go test -race -count=1 ./...

# 端到端演示：注入 2 次失败后重试收敛，并打印审计轨迹
go run ./cmd/demo
```

### 测试覆盖矩阵

| 需求点 | 测试 |
|---|---|
| 循环依赖 / 缺失依赖 / 非法声明，原因可区分 | `TestValidateDeclarations` |
| 依赖波次顺序 | `TestTopoWavesOrdering` |
| 依赖创建顺序 + 重复触发幂等、无重复副作用 | `TestDependencyOrderingAndIdempotence` |
| 规格漂移只更新一次、再触发 noop | `TestSpecDriftTriggersExactlyOneUpdate` |
| 逆依赖顺序删除 | `TestDeleteRespectsReverseDependencyOrder` |
| 并发调和无重复副作用 | `TestConcurrentReconcileIsIdempotent`、`TestConcurrentStressNoDuplicateSideEffects` |
| 陈旧世代被拒且不回滚新状态 | `TestStaleGenerationRejected` |
| 越权 / 保护 / 版本不兼容 / 版本降级，原因可区分 | `TestFailureReasonsAreDistinguishable` |
| 可追踪审计记录 | `TestAuditTrailRecordsDecisions` |
| 部分失败重试、下游阻断、最终幂等 | `TestPartialFailureRetryConverges`、`TestDependencyFailureBlocksDependents` |
| 进程重启恢复 | `TestRestartResumesFromDurableState`、`TestRestartAfterPartialFailure`、`TestReconcileIdempotentAcrossDriftAndRestart` |
| 外部修改后自愈（改回声明态，之后 noop） | `TestExternalModificationHeals` |
| 外部删除后重建（含依赖场景，无副作用泄漏） | `TestExternalDeletionRecreates` |
| 更新连续失败如实上报、跨轮重试直到真正生效（仅一次实际写入） | `TestUpdateFailureRetriesUntilEffective` |
| 重启后带漂移按实况修复 | `TestRestartReconcilesFromLiveBackendState` |
| 重启后带未生效写入继续重试直至生效 | `TestRestartWithPendingFailureKeepsRetrying` |
| 存储原子往返/深拷贝/世代单调 | `pkg/storage/store_test.go` |
| 后端重复建拒绝/故障注入自愈 | `pkg/applier/fake_test.go` |

单测统一要求打印**输入状态**（`INPUT ...`，如声明列表、世代、后端版本、并发数）与
**关键判定依据**（`JUDGE ...` / 引擎的 `basis=...` 日志），失败时可直接对照定位。
