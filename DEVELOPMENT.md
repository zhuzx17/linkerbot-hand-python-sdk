# Response attribution

Discovery and transaction response attribution use Transport's optional
`claimed_order`/`received_order` metadata when available. This preserves send
and receive ordering on low-resolution Windows clocks without discarding valid
replies during a batch. Older transports retain the timestamp fallback; equal
timestamps remain ambiguous on that compatibility path.

The complete ordering fix requires a Transport version that supplies this
metadata. This compatibility fallback does not remove the ambiguity in older
Transport versions.

## 中文说明

设备发现和事务回复优先使用 Transport 的可选事件序号判断 claim 前后关系，避免
Windows 低精度时钟误采信旧回复；旧传输仍保留时间戳兼容路径，相同时间戳仍有歧义。
完整修复需要配套提供事件序号的 Transport 版本；保留旧版本兼容不代表旧版本的
时间戳歧义已解决。
