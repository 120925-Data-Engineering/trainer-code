
# Building the Spark Environment 

**Goal:** Install Java, Apache Spark, and PySpark, and run your first distributed job locally.
**System:** WSL2 (Ubuntu) or macOS.

-----

## Part 1: Prerequisites (Java JDK)

Spark runs on the Java Virtual Machine (JVM). We need **Java 11**.

### For WSL2 (Ubuntu) Users

1.  Update your package list:
    ```bash
    sudo apt-get update
    ```
2.  Install OpenJDK 11:
    ```bash
    sudo apt-get install openjdk-11-jdk -y
    ```
3.  Verify installation:
    ```bash
    java -version
    # Output should look like: openjdk version "11.0..."
    ```

### For macOS Users (Apple Silicon or Intel)

*Note: We assume you have Homebrew installed.*

1.  Install OpenJDK 11:
    ```zsh
    brew install openjdk@11
    ```
2.  **Crucial Step for Mac:** Link it so the system sees it.
    ```zsh
    sudo ln -sfn /opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
    ```
3.  Verify installation:
    ```zsh
    java -version
    ```

-----

## Part 2: Installing Apache Spark (The Engine)

We will install Spark manually to `/opt/spark`.

### Step 2.1: Download

Run these commands in your terminal:

1.  **Download Spark 3.5.0** (Pre-built for Hadoop 3):

    ```bash
    wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
    ```

    *(Mac users: If you do not have `wget`, use `curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz`)*

2.  **Unpack the file:**

    ```bash
    tar -xvf spark-3.5.0-bin-hadoop3.tgz
    ```

### Step 2.2: Move to the standard location

```bash
sudo mv spark-3.5.0-bin-hadoop3 /opt/spark
```

-----

## Part 3: Environment Variables (The Glue)

We need to tell your shell where to find Java and Spark.

### Step 3.1: Edit the Config File

Open your config file using `nano`:

  * **WSL2:** `nano ~/.bashrc`
  * **Mac:** `nano ~/.zshrc`

Scroll to the **very bottom** of the file and paste the following block exactly:

```bash
# --- SPARK CONFIGURATION ---

# 1. Java Location (Adjusted for OS automatically below)
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # WSL2 / Linux
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home
fi

# 2. Spark Location
export SPARK_HOME=/opt/spark

# 3. Add Binaries to Path (So you can type 'pyspark' anywhere)
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# 4. Python Bindings (Force Python 3)
export PYSPARK_PYTHON=python3

# 5. [WSL2 ONLY] Fix for Networking Hangs
# If you are on Mac, this line is harmless but unnecessary.
export SPARK_LOCAL_IP=127.0.0.1
```

### Step 3.2: Save and Reload

1.  Press `Ctrl+O`, `Enter` to save.
2.  Press `Ctrl+X` to exit Nano.
3.  **Apply the changes:**
      * **WSL2:** `source ~/.bashrc`
      * **Mac:** `source ~/.zshrc`

-----

## Part 4: Python Setup (Virtual Environment)

To avoid system errors (PEP 668), we must create a virtual environment for our Python packages.

### Step 4.1: Install the venv tool (WSL2 Only)

Mac users usually have this built-in. WSL2 users need to run:

```bash
sudo apt-get install python3-venv -y
```

### Step 4.2: Create the Environment

Create a folder named `pyspark_env` in your home directory:

```bash
cd ~
python3 -m venv pyspark_env
```

### Step 4.3: Activate the Environment

Activating the environment redirects your `python` and `pip` commands to this folder.

```bash
source ~/pyspark_env/bin/activate
```

  * *Check:* Your command prompt should now have `(pyspark_env)` at the start.

### Step 4.4: Install PySpark

Now we can safely install the library.

```bash
pip install pyspark==3.5.0
```

### Step 4.5: Auto-Activate (Recommended)

To ensure this environment loads every time you open a terminal, add it to your config file.

1.  Open `.bashrc` (or `.zshrc`):
    ```bash
    nano ~/.bashrc
    ```
2.  Add this line at the **very bottom** (after the Spark variables):
    ```bash
    source ~/pyspark_env/bin/activate
    ```
3.  Save and Exit.
4.  Reload one last time: `source ~/.bashrc` (or `~/.zshrc`).

-----

## Part 5: The Sanity Check

Before writing code, verify that `python` points to your virtual env, not the system root.

Copy-paste this block into your terminal:

```bash
echo "--------------------------------"
echo "Checking Environment..."
echo "JAVA_HOME:  $JAVA_HOME"
echo "SPARK_HOME: $SPARK_HOME"
echo "Python Path: $(which python)"
echo "--------------------------------"
```

**Success Criteria:**

  * `JAVA_HOME`: Should not be empty.
  * `SPARK_HOME`: Should be `/opt/spark`.
  * `Python Path`: Should be `/home/username/pyspark_env/bin/python`.
      * *If it says `/usr/bin/python`, the virtual environment is NOT active.*

-----

## Part 6: Hello World (Your First Job)

Let's test if the Driver can talk to the Executor.

1.  Type `pyspark` in your terminal.
2.  Wait for the Spark logo (ASCII art) to appear.
3.  Run this Python code:

<!-- end list -->

```python
# 1. Create a simple list of data
data = [("James", 34), ("Anna", 20), ("Julia", 55)]

# 2. Convert it to a Distributed DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# 3. Trigger an Action (The JVM will wake up now)
df.show()
```

**Expected Output:**

```text
+-----+---+
| Name|Age|
+-----+---+
|James| 34|
| Anna| 20|
|Julia| 55|
+-----+---+
```

4.  Exit the shell by typing `exit()` or pressing `Ctrl+D`.