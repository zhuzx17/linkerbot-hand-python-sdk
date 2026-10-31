# 快速开始

## 1.获取源码

执行以下命令，从 GitHub 获取两个仓库的当前源码。

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

## 2.安装

在两个仓库共同的父目录（示例为 `linkerbot_sdk`）执行以下命令。命令使用新克隆的默认目录名；已有仓库的目录名若不同，请替换为实际路径。旧克隆的远程地址更新见[更新 SDK](./sdk-update.md)。

### Linux

请选择 `uv` 或 `pip` 中的一种方案安装 SDK。

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

## 3.配置通信接口

以下以经典 CAN 为例，使用 Linux SocketCAN 配置并启用通信接口。CAN FD 使用对应的 CAN FD 配置；Windows 的 PCAN、SLCAN，以及两平台的 MiniCANFD 无需执行这些命令。

```bash
sudo ip link set can0 down
sudo ip link set can0 type can bitrate 1000000
sudo ip link set can0 up
```

<a id="first-read"></a>

详细配置见[准备通信接口](./connection/prepare.md)。自动发现的适配器范围及 MiniCANFD 通道回退见[自动连接](./connection/automatic.md)。

## 4.读取首组角度值

确认当前仅连接一只灵巧手后，运行以下命令读取首组角度值：

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

[返回总览](./README.md#连接设备)
