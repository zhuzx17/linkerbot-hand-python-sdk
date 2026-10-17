# Development and CI

Follow the [organization contribution guide](https://github.com/linker-bot/.github/blob/main/CONTRIBUTING.md).
Install the current Transport source as described in the README, then run from
this repository in a virtual environment:

```bash
python -m pip install -r requirements/ci-lint.txt -r requirements/ci-test.txt
python -m pip install -e .
ruff check .
ruff format --check .
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 python -m pytest -p pytest_timeout -p pytest_cov.plugin \
  --timeout=60 --cov=linkerbot_hand --cov-report=term-missing:skip-covered \
  --cov-report=xml:reports/coverage.xml --junitxml=reports/pytest.xml
python -m build
```

The example uses Bash; in PowerShell set `$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = "1"`
and run the pytest command on one line. Test plugins are enabled explicitly to
avoid unrelated plugins from the local environment. Coverage and JUnit reports
are written to `reports/`; coverage is recorded without a minimum threshold.

GitHub Actions runs Ruff lint/format, ShellCheck, and actionlint on pull requests
and pushes to `main`. Offline tests use Python 3.10 and 3.13 on Linux and Windows;
the weekly run also tests Python 3.11 and 3.12. The package job builds the sdist
and wheel, installs the wheel, and runs `tools/check_installed.py` outside the
checkout to verify imports, typing metadata, and the license.

PRs and `main` updates also run the existing two-hand/two-arm integration suite
from Transport on the same Linux/Windows and Python matrix. This repository uses
the workflow commit (the merge result for PRs); Transport and the Arm SDK are
resolved once to exact commits and reused across the integration jobs. Each job
saves the three source revisions and JUnit report, and uses real Pinocchio with
simulated devices on four virtual CAN channels.

Weekly and manual runs also build and test the latest Hand ROS wrapper in
Humble and Jazzy Docker environments with this SDK. Transport is installed from
GitHub `main`; logs record the actual source commits. Reports and distributions
are retained as Actions artifacts for 14 days. These jobs need no Gitea access
or device credentials and do not publish releases.

Tests use simulated transports and protocol data. Physical CAN/CAN FD adapters,
device firmware, and movement require separate hardware validation.

Discovery and transaction response attribution use Transport's optional
`claimed_order`/`received_order` metadata when available. This preserves send
and receive ordering on low-resolution Windows clocks without discarding valid
replies during a batch. Older transports retain the timestamp fallback; equal
timestamps remain ambiguous on that compatibility path. Regression tests cover
identical timestamps and repeated complete angle queries between tactile fingers.
Asynchronous tests observe acknowledgement and publication events directly;
coarse elapsed timestamps and rapidly replaced snapshots are not event histories.

## 中文说明

先按 README 安装当前 Transport 源码，再使用固定的 CI 工具版本运行上述检查。
普通 CI 覆盖 Linux/Windows 和 Python 3.10、3.13；每周补测 3.11、3.12 及
Humble/Jazzy 下的最新 ROS 消费者。查看 Actions artifacts 中的测试、覆盖率与打包结果。
PR 与 `main` 更新同时运行 Transport 中已有的双手双臂联合测试：Hand 使用本次
工作流提交（PR 使用合并结果），Transport、Arm 在本轮联合矩阵内固定为同一组提交，
每个任务保存三个源码版本和 JUnit。测试使用真实 SDK、Pinocchio 与四路虚拟 CAN。
离线测试通过不能代替实机通信、固件与动作验证。

设备发现和事务回复优先使用 Transport 的可选事件序号判断 claim 前后关系，避免
Windows 低精度时钟误采信旧回复；旧传输仍保留时间戳兼容路径，相同时间戳仍有歧义。
触觉回归检查指间完整、有界的角度查询组，不假设 100 Hz 轮询只能发生一次。
异步回归直接观察 ACK 和发布事件，避免使用粗粒度耗时或瞬时快照推断事件历史。

## 在个人 fork 验证 CI

Actions 的手动运行入口支持 `dependency_owner` 和 `dependency_ref`：默认仍使用
`linker-bot` 依赖，以及原有的 main/配对提交规则。跨仓库改动可先将 Transport、
Hand/Arm Python SDK 和两个 ROS 仓库推送到个人 fork 的同名测试分支，再统一指定
个人账号与该分支，避免混用上游尚未更新的测试脚本。日志记录实际使用的源码提交。

联合任务始终测试本次 Hand 工作流的代码；上述参数仅选择其他仓库的依赖。
所选 Transport 提交必须包含 `.github/actions/four-devices` 共享执行步骤；首次合入时
先合入 Transport，再合入 Hand/Arm 工作流。

新 fork 的工作流需要启用 Actions，且默认分支上需要包含工作流文件，才能手动运行；
可以仅将个人 fork 的默认分支设为测试分支。上游默认分支不受影响。

手动运行时设置 `full_matrix=true`，会补齐 Python 3.11、3.12；ROS/SDK 消费者任务也会运行。
