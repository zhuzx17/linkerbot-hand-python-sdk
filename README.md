# Quick Start

English | [简体中文](README_zh.md)

## 1. Get the source

Get the current source of both repositories from GitHub.
Skip this step if you already have local source repositories.

### Linux

```bash
# Create and enter the source directory
mkdir -p linkerbot_sdk
cd linkerbot_sdk
# Clone both repositories
git clone https://github.com/linker-bot/linkerbot-transport.git
git clone https://github.com/linker-bot/linkerbot-hand-python-sdk.git
```

### Windows PowerShell

```powershell
# Create and enter the source directory
New-Item -ItemType Directory -Path linkerbot_sdk -Force
cd linkerbot_sdk
# Clone both repositories
git clone https://github.com/linker-bot/linkerbot-transport.git
git clone https://github.com/linker-bot/linkerbot-hand-python-sdk.git
```

## 2. Install

Run the following commands from the shared parent directory of both repositories
(`linkerbot_sdk` in this example). The commands use the default clone directory names;
substitute your actual paths if they differ. For existing clones that use the old URLs,
see [Updating the SDK](./docs/en/src/reference/sdk-update.md).

### Linux

Choose either `uv` or `pip` to install the SDK.

uv:

```bash
# Create a virtual environment (the SDK supports Python 3.10+)
uv venv py-linkerbot --python 3.12
# Activate the environment
source py-linkerbot/bin/activate
# Install both packages from source
uv pip install -e "./linkerbot-transport[all]"
uv pip install -e ./linkerbot-hand-python-sdk
```

pip:

```bash
# Create a virtual environment (the SDK supports Python 3.10+)
python3.12 -m venv py-linkerbot
# Activate the environment
source py-linkerbot/bin/activate
# Update the installation tools
python -m pip install --upgrade pip setuptools wheel
# Install both packages from source
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

### Windows

```powershell
# Create a virtual environment (the SDK supports Python 3.10+)
py -3.12 -m venv py-linkerbot
# Activate the environment
.\py-linkerbot\Scripts\Activate.ps1
# Update the installation tools
python -m pip install --upgrade pip setuptools wheel
# Install both packages from source
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

## 3. Configure the communication interface

For Classical CAN on Linux, configure and enable the SocketCAN interface as follows.
Use the CAN FD configuration for CAN FD hands. Windows PCAN and SLCAN, and MiniCANFD
on both platforms, do not require these commands.

```bash
sudo ip link set can0 down
sudo ip link set can0 type can bitrate 1000000
sudo ip link set can0 up
```

See [Interface preparation](./docs/en/src/reference/connection/prepare.md) for details.
For automatic discovery, supported adapters and MiniCANFD channel fallback, see
[Automatic connection](./docs/en/src/reference/connection/automatic.md).

<a id="first-read"></a>

## 4. Read the first angles

Connect exactly one hand, then run the following Python code to read its first angle sample:

```python
from linkerbot_hand import discover_hands
# Identify the device; require_one() requires exactly one candidate
hand = discover_hands().require_one().create_hand_device()
try:
    # Open the channel, verify identity and read the baseline
    hand.open()
    # Read raw protocol angles
    print(hand.get_angles(timeout=1.0).raw)
finally:
    # Release the connection and background resources
    hand.close()
```

[Full user manual](./docs/en/src/reference/README.md): connection settings and examples for each model.

## Contributing and license

See [Development and CI](DEVELOPMENT.md) for local checks and the test matrix.

English and Chinese contributions are welcome. Follow the
[Linkerbot contribution guide](https://github.com/linker-bot/.github/blob/main/CONTRIBUTING.md).
Report vulnerabilities privately using the
[organization security policy](https://github.com/linker-bot/.github/blob/main/SECURITY.md).

Licensed under the [MIT License](LICENSE).
