# Setting Up Streamlit

## Learning Objectives
- Set up a Python virtual environment for Streamlit
- Install Streamlit and required dependencies
- Run your first Streamlit application
- Understand the development workflow

## Why This Matters

A proper development environment prevents dependency conflicts and ensures consistent behavior across machines. Virtual environments isolate project dependencies, making your Streamlit apps reproducible and maintainable.

## Prerequisites

Before starting:
- Python 3.8 or higher installed
- pip (Python package manager)
- A code editor (VS Code recommended)
- Terminal or command prompt access

### Verifying Python Installation

**Windows:**
```powershell
python --version
```

**macOS/Linux:**
```bash
python3 --version
```

You should see Python 3.8.x or higher.

## Creating a Virtual Environment

### Why Virtual Environments?

- Isolate project dependencies
- Avoid conflicts between projects
- Reproducible deployments
- Easy cleanup

### Windows Setup

```powershell
# Navigate to your project directory
cd C:\Users\YourName\Projects\streamlit-app

# Create virtual environment
python -m venv venv

# Activate virtual environment
.\venv\Scripts\Activate

# Verify activation (prompt should show venv)
```

### macOS/Linux Setup

```bash
# Navigate to your project directory
cd ~/projects/streamlit-app

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Verify activation (prompt should show venv)
```

### Deactivating

When finished working:
```
deactivate
```

## Installing Streamlit

With your virtual environment activated:

```bash
# Install Streamlit
pip install streamlit

# Verify installation
streamlit --version
```

### Common Dependencies

Install additional packages you will likely need:

```bash
pip install pandas numpy
pip install plotly altair
pip install snowflake-connector-python
```

### Saving Dependencies

Create a requirements file for reproducibility:

```bash
pip freeze > requirements.txt
```

To install from requirements:
```bash
pip install -r requirements.txt
```

## Project Structure

Recommended structure for a Streamlit project:

```
streamlit-app/
  |-- venv/                 # Virtual environment (do not commit)
  |-- .streamlit/           # Streamlit configuration
  |     |-- config.toml     # App configuration
  |     |-- secrets.toml    # Credentials (do not commit)
  |-- pages/                # Multi-page app pages
  |     |-- 1_Dashboard.py
  |     |-- 2_Analysis.py
  |-- app.py                # Main application
  |-- requirements.txt      # Dependencies
  |-- .gitignore           # Git ignore file
```

### .gitignore Contents

```
venv/
.streamlit/secrets.toml
__pycache__/
*.pyc
.env
```

## Running Your First App

### Create the App

Create `app.py`:

```python
import streamlit as st

st.title("My First Streamlit App")
st.write("Setup complete!")
```

### Run the App

```bash
streamlit run app.py
```

Expected output:
```
You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.1.100:8501
```

Your browser should open automatically.

### Stopping the App

Press `Ctrl+C` in the terminal.

## Development Workflow

### Hot Reload

Streamlit watches for file changes:
1. Edit your Python file
2. Save the file
3. Browser shows "Source file changed"
4. Click "Rerun" or enable "Always rerun"

### Debug Mode

For detailed error messages:
```bash
streamlit run app.py --logger.level=debug
```

### Custom Port

Run on a different port:
```bash
streamlit run app.py --server.port 8080
```

## Configuration

### config.toml

Create `.streamlit/config.toml` for app settings:

```toml
[theme]
primaryColor = "#FF4B4B"
backgroundColor = "#FFFFFF"
secondaryBackgroundColor = "#F0F2F6"
textColor = "#262730"

[server]
port = 8501
headless = true

[browser]
gatherUsageStats = false
```

### Theme Options

| Setting | Description |
|---------|-------------|
| `primaryColor` | Accent color for widgets |
| `backgroundColor` | Main background |
| `secondaryBackgroundColor` | Sidebar and card backgrounds |
| `textColor` | Default text color |
| `font` | Font family (sans serif, serif, monospace) |

## Secrets Management

Store sensitive data in `.streamlit/secrets.toml`:

```toml
[snowflake]
account = "xy12345.us-east-1"
user = "username"
password = "password"
warehouse = "COMPUTE_WH"
database = "ANALYTICS"
schema = "GOLD"
```

Access in code:
```python
import streamlit as st

account = st.secrets["snowflake"]["account"]
```

**Important:** Never commit secrets.toml to version control.

## Multi-Page Apps

Create a `pages/` directory for multi-page apps:

```
streamlit-app/
  |-- app.py               # Home page
  |-- pages/
        |-- 1_Dashboard.py  # First page
        |-- 2_Analysis.py   # Second page
```

Pages appear in sidebar automatically. Numeric prefixes control order.

## IDE Setup (VS Code)

### Recommended Extensions

- Python (Microsoft)
- Pylance
- Python Environment Manager

### launch.json for Debugging

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Streamlit",
      "type": "python",
      "request": "launch",
      "module": "streamlit",
      "args": ["run", "app.py"]
    }
  ]
}
```

## Common Setup Issues

| Issue | Solution |
|-------|----------|
| `streamlit: command not found` | Ensure virtual environment is activated |
| Port already in use | Use `--server.port` with different port |
| Module not found | Install missing package with pip |
| Permission denied | Run terminal as administrator (Windows) |

## Summary

- Virtual environments isolate project dependencies
- Install Streamlit with `pip install streamlit`
- Run apps with `streamlit run app.py`
- Configure apps with `.streamlit/config.toml`
- Store secrets in `.streamlit/secrets.toml` (never commit)
- Use `pages/` directory for multi-page apps

## Additional Resources

- [Streamlit Installation](https://docs.streamlit.io/library/get-started/installation) - Official guide
- [Configuration Options](https://docs.streamlit.io/library/advanced-features/configuration) - All config options
- [Secrets Management](https://docs.streamlit.io/library/advanced-features/secrets-management) - Secure credentials
