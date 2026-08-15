# Batcher

将零散的单次请求扇入为批量操作，并把结果按位置归还各调用方的库。

## Language

**Item**:
一次进入批处理器的单条请求，由一个调用方同步等待其结果。
_Avoid_: request（与下游批量接口的请求混淆）、task、entry

**Batch**:
一次交付给批次函数执行的 Item 集合，结果按位置与 Item 对位。
_Avoid_: group、chunk

**Batch Function**:
用户提供的批量执行函数，接收整个 Batch（恒非空），返回等长的结果切片；返回非 nil 错误即整批失败。
_Avoid_: handler、executor

**Formation**:
从第一条 Item 进入空批到 Flush 触发的组批阶段；时间间隔在 Formation 起点开始计时，是尽力而为的软上限。
_Avoid_: collection window、timer window

**Flush**:
将组批中的 Batch 交付给批次函数执行的动作；触发条件为批大小上限或时间间隔，先到先触发。仅有数据才实际触发：空批不计时、不唤醒，绝不产生对批次函数的空调用。
_Avoid_: dispatch、fire、commit

**Waiter**:
调用 Do 并阻塞等待自己那份结果返回的调用方。
_Avoid_: caller（过于泛化）、consumer

**Batch Error**:
批次函数返回的非 nil 错误，或其 panic 经 recover 转化的错误；广播给该批全部 Waiter，不重试。单条 Item 的业务失败由用户在结果类型中自行表达，与 Batch Error 无关。
_Avoid_: partial error

**In-flight Batch**:
已 Flush、批次函数尚未返回的 Batch；同一时刻至多一个。
