#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"
REQUIRED_PYTHON="3.12"

echo "==> Checking Python ($REQUIRED_PYTHON)..."

find_python312() {
    for candidate in python3.12 python3 python; do
        if command -v "$candidate" &>/dev/null; then
            local ver
            ver=$("$candidate" --version 2>&1 | awk '{print $2}')
            local major_minor
            major_minor=$(echo "$ver" | cut -d. -f1,2)
            if [ "$major_minor" = "$REQUIRED_PYTHON" ]; then
                echo "$candidate"
                return 0
            fi
        fi
    done
    return 1
}

PYTHON=$(find_python312 || true)

if [ -z "$PYTHON" ]; then
    echo "    Python $REQUIRED_PYTHON not found"

    if [[ "$OSTYPE" == "darwin"* ]] && command -v brew &>/dev/null; then
        echo "==> Installing Python $REQUIRED_PYTHON via Homebrew..."
        brew install python@3.12
        PYTHON=$(find_python312 || true)
        if [ -z "$PYTHON" ]; then
            echo "ERROR: Python $REQUIRED_PYTHON still not found after install"
            exit 1
        fi
    else
        echo "ERROR: Please install Python $REQUIRED_PYTHON first"
        echo "  macOS: brew install python@3.12"
        echo "  Ubuntu/Debian: sudo apt install python3.12 python3.12-venv"
        exit 1
    fi
fi

echo "    Using: $PYTHON ($("$PYTHON" --version 2>&1))"

# Check venv module
if ! "$PYTHON" -m venv --help &>/dev/null; then
    echo "ERROR: venv module missing, reinstall Python $REQUIRED_PYTHON"
    exit 1
fi

# Create venv
NEED_CREATE=false

if [ -d "$VENV_DIR" ]; then
    VENV_PY_VER=$("$VENV_DIR/bin/python" --version 2>&1 | awk '{print $2}' | cut -d. -f1,2 || true)
    if [ "${VENV_PY_VER:-}" != "${REQUIRED_PYTHON:-}" ]; then
        echo "==> .venv version mismatch (${VENV_PY_VER:-unknown}), recreating..."
        rm -rf "$VENV_DIR"
        NEED_CREATE=true
    else
        echo "==> .venv exists, skipping creation"
    fi
else
    NEED_CREATE=true
fi

if [ "$NEED_CREATE" = true ]; then
    echo "==> Creating virtual environment: $VENV_DIR"
    "$PYTHON" -m venv "$VENV_DIR"
fi

# Activate and install
echo "==> Activating venv and installing dependencies..."
source "$VENV_DIR/bin/activate"

echo "==> Upgrading pip..."
pip install --upgrade pip -q

echo "==> Installing requirements..."
pip install -r "$PROJECT_DIR/requirements.txt"

echo ""
echo "============================================"
echo "  Done! Activate: source .venv/bin/activate"
echo "============================================"
