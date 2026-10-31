# 更新 SDK

如果你已经安装或使用过 SDK，需要更新本地源码，请在两个仓库共同的父目录（例如 `linkerbot_sdk`）执行以下命令。命令使用 GitHub 新克隆的默认目录名；如果已有 hand 仓库目录叫 `linkerbot-hand` 或 `linkerbot-hand-python`，请将命令中的 `linkerbot-hand-python-sdk` 替换为实际目录名，本地目录无需重命名。

如果现有克隆仍使用旧仓库地址，先更新 `origin`：

```bash
git -C linkerbot-transport remote set-url origin https://github.com/linker-bot/linkerbot-transport.git
git -C linkerbot-hand-python-sdk remote set-url origin https://github.com/linker-bot/linkerbot-hand-python-sdk.git
```

将两个仓库切换到 `main`，并更新到最新源码：

```bash
# 更新传输层源码
git -C linkerbot-transport switch main
git -C linkerbot-transport pull --ff-only origin main
# 更新设备 SDK 源码
git -C linkerbot-hand-python-sdk switch main
git -C linkerbot-hand-python-sdk pull --ff-only origin main
```

如果 Git 提示本地修改或分支分歧，请先处理相关问题，再继续更新。

源码更新完成后，请在当前目录执行对应命令，更新环境中的 SDK。

## Linux

原先使用 uv 安装：

```bash
# 激活 SDK 所在虚拟环境
source py-linkerbot/bin/activate
# 安装两个源码包及适配器依赖
uv pip install -e "./linkerbot-transport[all]"
uv pip install -e ./linkerbot-hand-python-sdk
```

原先使用 pip 安装：

```bash
# 激活 SDK 所在虚拟环境
source py-linkerbot/bin/activate
# 安装两个源码包及适配器依赖
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

## Windows PowerShell

```powershell
# 激活 SDK 所在虚拟环境
.\py-linkerbot\Scripts\Activate.ps1
# 安装两个源码包及适配器依赖
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

更新 SDK 后，请重新运行使用 SDK 的 Python 程序。
