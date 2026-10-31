# Updating the SDK

If you have already installed or used the SDK and need to update the local source, run the following commands from the shared parent directory of both repositories (for example, `linkerbot_sdk`). The commands use the default directory names for new GitHub clones. If your existing hand directory is named `linkerbot-hand` or `linkerbot-hand-python`, substitute that name for `linkerbot-hand-python-sdk` in the commands; you do not need to rename the local directory.

If your existing clones still use the old repository URLs, update `origin` first:

```bash
git -C linkerbot-transport remote set-url origin https://github.com/linker-bot/linkerbot-transport.git
git -C linkerbot-hand-python-sdk remote set-url origin https://github.com/linker-bot/linkerbot-hand-python-sdk.git
```

Switch both repositories to `main` and update them to the latest source:

```bash
# Update the transport source
git -C linkerbot-transport switch main
git -C linkerbot-transport pull --ff-only origin main
# Update the hand SDK source
git -C linkerbot-hand-python-sdk switch main
git -C linkerbot-hand-python-sdk pull --ff-only origin main
```

If Git reports local changes or diverging branches, resolve those issues before continuing.

After updating the source, run the appropriate commands from the current directory to update the SDK in your environment.

## Linux

If originally installed with uv:

```bash
# Activate the SDK virtual environment
source py-linkerbot/bin/activate
# Install both source packages and adapter dependencies
uv pip install -e "./linkerbot-transport[all]"
uv pip install -e ./linkerbot-hand-python-sdk
```

If originally installed with pip:

```bash
# Activate the SDK virtual environment
source py-linkerbot/bin/activate
# Install both source packages and adapter dependencies
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

## Windows PowerShell

```powershell
# Activate the SDK virtual environment
.\py-linkerbot\Scripts\Activate.ps1
# Install both source packages and adapter dependencies
python -m pip install -e "./linkerbot-transport[all]"
python -m pip install -e ./linkerbot-hand-python-sdk
```

After updating the SDK, restart any Python programs that use it.
