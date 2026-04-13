from __future__ import annotations

import argparse
import contextlib
import os
import socket
import subprocess
import time
import venv
import webbrowser
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parent
VENV = ROOT / ".venv"
STAMP = VENV / ".holiday_hunter_bootstrap_complete"
REQUIREMENTS = ROOT / "requirements.txt"
LOGS = ROOT / "logs"
LOGS.mkdir(exist_ok=True)
SERVER_LOG = LOGS / "server.log"
BOOTSTRAP_LOG = LOGS / "bootstrap.log"
LAST_URL = LOGS / "last_url.txt"
DEFAULT_HOST = os.getenv("HOLIDAY_HUNTER_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.getenv("HOLIDAY_HUNTER_PORT", "8000"))


def log(message: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(line)
    with BOOTSTRAP_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def is_windows() -> bool:
    return os.name == "nt"


def venv_python() -> Path:
    return VENV / ("Scripts/python.exe" if is_windows() else "bin/python")


def ensure_venv() -> None:
    if venv_python().exists():
        return
    log("Creating virtual environment...")
    venv.EnvBuilder(with_pip=True).create(VENV)


def run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    log("> " + " ".join(str(x) for x in cmd))
    return subprocess.run(cmd, cwd=str(ROOT), check=check)


def ensure_dependencies(force: bool = False) -> None:
    py = str(venv_python())
    if force or not STAMP.exists():
        log("Installing dependencies...")
        run([py, "-m", "pip", "install", "--upgrade", "pip"])
        run([py, "-m", "pip", "install", "-r", str(REQUIREMENTS)])
        run([py, "-m", "playwright", "install", "chromium"])
        STAMP.write_text(str(int(time.time())), encoding="utf-8")


def port_in_use(host: str, port: int) -> bool:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.settimeout(0.5)
        return sock.connect_ex((host, port)) == 0


def find_open_port(host: str, start_port: int, attempts: int = 50) -> int:
    for port in range(start_port, start_port + attempts):
        if not port_in_use(host, port):
            return port
    raise RuntimeError("Could not find an open port for the local app.")


def wait_for_server(url: str, timeout: int = 90) -> bool:
    health = url.rstrip("/") + "/api/health"
    end = time.time() + timeout
    while time.time() < end:
        try:
            with urlopen(health, timeout=2) as resp:
                if resp.status == 200:
                    return True
        except Exception:
            time.sleep(1)
    return False


def start_server(host: str, port: int, reload_mode: bool) -> subprocess.Popen:
    py = str(venv_python())
    cmd = [py, "-m", "uvicorn", "main:app", "--host", host, "--port", str(port)]
    if reload_mode:
        cmd.append("--reload")
    log(f"Starting server on {host}:{port}")
    server_log = SERVER_LOG.open("a", encoding="utf-8")
    return subprocess.Popen(cmd, cwd=str(ROOT), stdout=server_log, stderr=subprocess.STDOUT)


def open_browser(url: str) -> None:
    log(f"Opening browser at {url}")
    webbrowser.open(url)


def open_desktop_window(url: str) -> bool:
    try:
        import webview  # type: ignore
    except Exception as exc:
        log(f"Desktop window unavailable ({exc!r}); falling back to browser.")
        return False

    log(f"Opening desktop window at {url}")
    webview.create_window("AI Holiday Hunter", url, width=1500, height=980, min_size=(1180, 760))
    webview.start()
    return True


def create_shortcuts() -> None:
    if not is_windows():
        return
    silent = ROOT / "START_AI_Holiday_Hunter_Silent.vbs"
    silent.write_text(
        'Set WshShell = CreateObject("WScript.Shell")\n'
        'WshShell.Run chr(34) & "START_AI_Holiday_Hunter.bat" & chr(34), 0\n'
        'Set WshShell = Nothing\n',
        encoding="utf-8",
    )
    desktop_silent = ROOT / "START_AI_Holiday_Hunter_Desktop_Silent.vbs"
    desktop_silent.write_text(
        'Set WshShell = CreateObject("WScript.Shell")\n'
        'WshShell.Run chr(34) & "START_AI_Holiday_Hunter_Desktop.bat" & chr(34), 0\n'
        'Set WshShell = Nothing\n',
        encoding="utf-8",
    )


def write_last_url(url: str) -> None:
    LAST_URL.write_text(url, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--desktop", action="store_true", help="Open in a desktop app window when available.")
    parser.add_argument("--force-setup", action="store_true", help="Force reinstall of dependencies.")
    parser.add_argument("--no-browser", action="store_true", help="Do not open a browser automatically.")
    args = parser.parse_args()

    host = DEFAULT_HOST
    port = find_open_port(host, DEFAULT_PORT)
    url = f"http://{host}:{port}"

    create_shortcuts()
    ensure_venv()
    ensure_dependencies(force=args.force_setup or os.getenv("HOLIDAY_HUNTER_FORCE_SETUP", "").lower() in {"1", "true", "yes"})

    proc = start_server(host, port, reload_mode=not args.desktop)
    try:
        if wait_for_server(url):
            write_last_url(url)
            if not args.no_browser:
                opened_desktop = args.desktop and open_desktop_window(url)
                if not opened_desktop:
                    open_browser(url)
            if not args.desktop:
                log("Server running. Press Ctrl+C in this window to stop it.")
                proc.wait()
        else:
            log("Server did not report healthy in time. Check logs/server.log")
            return 1
        return int(proc.returncode or 0)
    except KeyboardInterrupt:
        log("Stopping server...")
        return 0
    finally:
        with contextlib.suppress(Exception):
            proc.terminate()
            proc.wait(timeout=10)
        with contextlib.suppress(Exception):
            proc.kill()


if __name__ == "__main__":
    raise SystemExit(main())
