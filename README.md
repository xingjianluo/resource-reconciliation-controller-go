# resource-reconciliation-controller-go

一个**本地资源编排与调和控制器**：接收一组资源声明（Declaration），让本地“后端世界”
最终收敛到声明所描述的目标状态，并保证该过程在依赖关系、并发更新、失败重试和进程
重启下结果一致、幂等、可解释（每次关键判定都有日志与可追踪审计记录）。

从第二版起，调和**以后端实况（live state）为唯一满足依据**：每一轮都会逐个资源向后端
读取它当前的真实内容，再决定“已满足 / 需要修复 / 需要重建 / 不可观测”。运维在后端
直接改配置、误删对象，或某次创建/更新其实没有生效，都不会因为控制器“上一轮成功过”
而被掩盖——下一轮调和必然发现并收敛。

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

### 以后端实况为准（Live State is Source of Truth）

每一轮调和、对每个待收敛资源，引擎都先调用 `Applier.Get(ctx, key)` 读取后端当前实况，
再依据实况（而不是本地快照里的 `Phase/SpecHash`）做判定：

| 后端实况 | 本地记录 | 判定与动作 |
|---|---|---|
| 读不到（超时/故障） | 任意 | **失败** `BackendUnavailable`，不做任何写入，后续每轮继续重试；本地说 Ready 也不能掩盖 |
| 对象不存在 | Ready/Failed/无 | **CREATE**：从未建成、崩溃丢失或被外部删除，一律补建 |
| 实况哈希 == 声明哈希 | 任意 | **NOOP**：唯一的“已满足”判据；若本地是 Failed/无记录，则无副作用地把簿记校正为 Ready（审计记 `adopt`） |
| 实况哈希 != 声明哈希 | 任意 | **UPDATE**：外部篡改的漂移、或上一次更新根本没写进去，都做恰好一次纠正写入 |

要点：

- **外部漂移自愈**：被别人改了 → 下一轮观测到 `liveHash != desiredHash`，做一次纠正
  `update`；被删了 → `liveExists=false`，做一次 `create` 补建。修好之后再跑一轮是纯
  noop，不产生多余写入。
- **失败如实上报、持续重试**：创建/更新报错时本轮进入 `ReconcileResult.Failed`，落
  `Phase=Failed` 与审计记录；**失败时绝不会把“未到达的目标哈希”写进簿记**，因此之后
  无论隔多少轮、中间别的资源是否成功，只要后端实况还没达标就继续重试，直到某一次真正
  写成功——那一刻只发生一次实际写入，此后各轮都是 noop。杜绝“没生效却报成功、再也不
  重试”。
- **重启后按实况重新判定**：重启复用状态文件后，依然逐个资源读后端实况，不凭重启前的
  Ready/Failed 下结论；带着外部漂移或“未生效写入”重启，都会被重新发现并修复。

### 幂等收敛的保证

- **不重复副作用**：提交由互斥锁串行化，波内有屏障；后端自身也拒绝重复 `Create`。
  并发/重复触发同一目标，每个资源最终只有 1 次 `create`（见并发与压测测试）。
- **满足态不改写**：声明经规范化（依赖排序）后做 SHA-256 摘要（`SpecHash`）。
  以后端实况的摘要与目标摘要比较：相等即判 `noop`，不调用后端 `Update`；漂移/删除
  则各做恰好一次纠正写入。
- **陈旧更新不覆盖**：旧世代在入口即被拒；同一资源的更低声明版本（版本降级）也被拒。

每个资源每轮的输入实况与判定依据都会打印到日志：`[observe] GET ... basis=live-truth
liveExists=.. liveHash=.. desiredHash=.. storedPhase=..`，以及
`[apply] CREATE|UPDATE|NOOP|ADOPT ... basis=backend-missing|external-drift|live-hash-equal`，
同时写入审计流。

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
| 后端实况不可读 | `BackendUnavailable` | `Get` 观测失败（超时/宕机），不凭本地状态宣称满足，后续每轮重试 |
| 存储不可用 | `StorageUnavailable` | 文件原子写失败时包装返回 |

所有拒绝都通过 append-only **审计流**（`AuditEvent`：世代、资源、动作、原因、消息、序号、时间）
落盘，可在状态文件中追踪。

## 失败恢复与重启策略

- **部分失败重试**：失败资源持久化为 `Phase=Failed` 并带失败条件，且簿记哈希**不会**
  提前更新为未到达的目标哈希；健康资源保持 `Ready`。再次调和时每轮都重新读取后端
  实况：健康资源因实况哈希一致为 `noop`（跨轮不会被重复写入），未达标资源继续
  create/update；后端故障“自愈”后即收敛，真正写成功的那一刻只发生一次写入。
- **外部漂移/删除自愈**：见“以后端实况为准”。被外部改掉的纠正、被外部删掉的补建，
  修复后再跑一轮纯 noop。
- **后端不可观测**：`Get` 失败记 `BackendUnavailable`，本轮不写、如实失败，恢复观测
  后继续正常收敛。
- **进程重启恢复**：存储对每次变更做原子快照（同目录临时文件 → `fsync` → `rename`），
  崩溃只会留下旧文件或新文件，不会产生半个文件。重启后重新打开同一个 store：
  - 世代恢复 → 旧世代依旧被拒；
  - 逐个资源重新读后端实况判定 → 已满足的判 `noop`，不重复创建；
  - 崩溃前缺失的资源、重启时已存在的外部漂移、或崩溃前未生效的写入 → 都会被重新
    发现并补齐/纠正，已经成功的资源不会重建。
- **删除安全性**：逆波次删除 + 引用检查；仍被（保留中的或期望中的）资源引用的依赖不会被删除。

## 兼容范围

- **Applier 接口**：后端需实现 `Create / Update / Delete / Exists / Get / APIVersion`。
  `Get` 返回 `(declaration, exists, error)`：`exists=false` 表示对象确实不存在；
  `error!=nil` 表示无法观测，控制器不会在此情况下推断满足。
- 保护、越权、版本兼容性规则在实况判定下全部保留：他人所有（以后端实况上的 owner
  注解为准）、`protected: "true"` 的内容变更、后端 API 版本过低、声明版本降级，仍被
  明确拒绝（`Force` 可放行保护/越权类）。注意：把一个已存在对象**修回声明原样**属于
  自愈，不要求 force；只有改变声明本身（向受保护对象写入新内容）才会被保护规则拦截。
- 世代单调、依赖波次、并发不产生重复副作用等原有行为保持不变。

## 本地验证方法

要求 Go 1.25+（开发使用 Go 1.26）。

```bash
# 编译
go build ./...

# 全量单测（-v 可看到每个用例打印的 INPUT 状态与 JUDGE 关键判定依据）
go test ./... -v

# 竞态检测（并发调和场景）
go test -race -count=1 ./...

# 端到端演示：注入 2 次失败后重试收敛，再现场外部漂移自愈，并打印审计
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
| 外部篡改后自愈（一次纠正 + 再跑 noop） | `TestExternalDriftSelfHeals` |
| 外部删除后补建（一次重建 + 再跑 noop） | `TestExternalDeleteRecreated` |
| 更新连续失败如实上报、恢复后恰好一次生效 | `TestUpdateFailuresRetriedUntilActuallyEffective` |
| 重启后带着漂移/未生效状态继续调和 | `TestRestartRejudgesByLiveState` |
| 后端不可观测时如实失败、恢复后续跑 | `TestBackendUnobservableFailsAndRetries` |
| 存储原子往返/深拷贝/世代单调 | `pkg/storage/store_test.go` |
| 后端重复建拒绝/故障注入自愈 | `pkg/applier/fake_test.go` |

单测统一要求打印**输入状态**（`INPUT ...`，如声明列表、世代、后端版本、并发数）与
**关键判定依据**（`JUDGE ...` / 引擎的 `basis=...` 日志），失败时可直接对照定位。
