# AI Holiday Hunter Windows App

## Best launch option now
- Double-click `START_AI_Holiday_Hunter_Desktop.bat`
- Or use `START_AI_Holiday_Hunter_Desktop_Silent.vbs` to hide the console window

## One-click setup
The launcher now:
- detects `py` or `python`
- creates `.venv` if missing
- installs dependencies
- installs Playwright Chromium
- starts the app automatically

## Turn it into a standalone Windows app
On a Windows machine with Python installed:
1. Double-click `build_windows_exe.bat`
2. Wait for PyInstaller to finish
3. Open `dist/AI Holiday Hunter`
4. Run the generated app

That build path creates a proper desktop-style Windows app wrapper. It still uses the same local engine under the hood.
