# Quick Start

Follow these steps to get started with LinkerHand.

## 1. Get the Source

Obtain the current source of both repositories from GitHub using the commands below. Skip this step if you already have local source repositories.

### Linux

```bash
# 创建并进入源码存放目录
mkdir -p linkerbot_sdk
cd linkerbot_sdk
# 获取两个源码仓库
git clone --branch main https://github.com/linker-bot/linkerbot-transport.git
git clone --branch main https://github.com/linker-bot/linkerbot-hand-python-sdk.git
```

### Windows PowerShell

```powershell
# 创建并进入源码存放目录
New-Item -ItemType Directory -Path linkerbot_sdk -Force
cd linkerbot_sdk
# 获取两个源码仓库
git clone --branch main https://github.com/linker-bot/linkerbot-transport.git
git clone --branch main https://github.com/linker-bot/linkerbot-hand-python-sdk.git
```

## 2. Install

Run the following commands from the shared parent directory of both repositories (`linkerbot_sdk` in this example). The commands use the default directory names for new clones; substitute your actual paths if they differ. For updating old clone URLs, see [Updating the SDK](./sdk-update.md).

### Linux

Choose either `uv` or `pip` to install the SDK.

uv：

```bash
# 创建虚拟环境（SDK 支持 Python 3.10+）
uv venv py-linkerbot --python 3.12
# 激活虚拟环境
source py-linkerbot/bin/activate
# 安装两个源码包
uv pip install -e "./linkerbot-transport[all]"
uv pip install -e ./linkerbot-hand-python-sdk
```

pip：

```bash
# 创建虚拟环境（SDK 支持 Python 3.10+）
python3.12 -m venv py-linkerbot
# 激活虚拟环境
source py-linkerbot/bin/activate
# 更新安装工具
python -m pip install --upgrade pip setuptools wheel
# 安装两个源码包
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

### Windows

```powershell
# 创建虚拟环境（SDK 支持 Python 3.10+）
py -3.12 -m venv py-linkerbot
# 激活虚拟环境
.\py-linkerbot\Scripts\Activate.ps1
# 更新安装工具
python -m pip install --upgrade pip setuptools wheel
# 安装两个源码包
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

## 3. Configure the Communication Interface

The following example configures and enables a Linux SocketCAN interface for Classical CAN:

```bash
sudo ip link set can0 down
sudo ip link set can0 type can bitrate 1000000
sudo ip link set can0 up
```

Use the CAN FD configuration for CAN FD hands. Windows PCAN and SLCAN, and MiniCANFD on both platforms, do not require these SocketCAN commands. See [Interface Preparation](./connection/prepare.md) and [Automatic Connection](./connection/automatic.md) for adapter selection and MiniCANFD channel fallback.
<a id="first-read"></a>

## 4. Read the First Set of Angles

Make sure exactly one dexterous hand is connected, then run the commands below to read its first set of angles:

```python
from linkerbot_hand import discover_hands
# 自动识别设备参数；require_one() 要求只接入一台设备
hand = discover_hands().require_one().create_hand_device()
try:
    # 打开通道，验证身份并读取基线
    hand.open()
    #读取原始角度
    print(hand.get_angles(timeout=1.0).raw)
finally:
    # 释放连接和后台资源
    hand.close()
```

[Back to overview](./README.md#connecting-devices)
