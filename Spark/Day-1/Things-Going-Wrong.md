# **PySpark on WSL2: Day 1 Troubleshooting Guide**

**"I typed `pyspark` and..."**

#### **1. Error: "Command 'pyspark' not found"**

* **The Symptom:** The terminal says `bash: pyspark: command not found` or suggests installing it via apt.
* **The Cause:** Linux doesn't know where you put the downloaded Spark folder. The `PATH` variable is missing the update.
* **The Fix:**
    1. Open your config: `nano ~/.bashrc`
    2. Ensure these lines are at the **very bottom**:

        ```bash
        export SPARK_HOME=/opt/spark
        export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
        ```

    3. **Crucial Step:** You must reload the file for changes to take effect:

        ```bash
        source ~/.bashrc
        ```

#### **2. Error: "JAVA\_HOME is not set"**

* **The Symptom:** You see a big error block starting with `JAVA_HOME is not set` even if you installed Java.
* **The Cause:** Installing Java isn't enough; Spark needs a specific environment variable pointing to the folder *containing* the `bin` directory.
* **The Fix:**
    1. Find where Java actually lives:

        ```bash
        update-alternatives --list java
        ```

        *(Copy the path up to the folder name, e.g., `/usr/lib/jvm/java-11-openjdk-amd64`—**do not** include `/bin/java` at the end)*.
    2. Add to `~/.bashrc`:

        ```bash
        export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
        ```

    3. Run `source ~/.bashrc`.

#### **3. Error: The Shell Hangs (Freezes) at Startup**

* **The Symptom:** You run `pyspark`, the ASCII art appears, but it hangs forever at `SparkSession available as 'spark'`. You cannot type.
* **The Cause:** WSL2 has dynamic IP addresses. Sometimes Spark tries to bind to an IP it can't reach.
* **The Fix:** Force Spark to use "localhost".
    1. Add this to `~/.bashrc`:

        ```bash
        export SPARK_LOCAL_IP=127.0.0.1
        ```

    2. Run `source ~/.bashrc`.

#### **4. Error: "Python not found" or Version Mismatch**

* **The Symptom:** `Exception: Python in worker has different version 3.x than that in driver 3.y...` OR `env: python: No such file or directory`.
* **The Cause:** Ubuntu calls Python `python3`, but older Spark scripts sometimes look for `python`. Or, your default `python` command points to Python 2.
* **The Fix:** Explicitly tell Spark which Python executable to use.
    1. Add to `~/.bashrc`:

        ```bash
        export PYSPARK_PYTHON=python3
        export PYSPARK_DRIVER_PYTHON=python3
        ```

    2. Run `source ~/.bashrc`.

#### **5. Error: "Permission Denied" when saving data**

* **The Symptom:** You try to save a DataFrame `df.write.csv("output")` and it crashes.
* **The Cause:** You are likely trying to write to a Windows folder (like `/mnt/c/Program Files/`) that Linux doesn't have permission to modify, or a root-owned Linux folder.
* **The Fix:**
  * **Quick Fix:** Always work inside your Linux home directory (`~` or `/home/yourname/`) for labs.
  * **If you must use Windows files:** Move the file *into* WSL first:

        ```bash
        cp /mnt/c/Users/Student/Downloads/data.csv ~/project_folder/
        ```

-----

### **The "Sanity Check" Script**


```bash
echo "--- DEBUG INFO ---"
echo "Java Location: $(which java)"
echo "Java Version: $(java -version 2>&1 | head -n 1)"
echo "Python Location: $(which python3)"
echo "SPARK_HOME: $SPARK_HOME"
echo "JAVA_HOME: $JAVA_HOME"
echo "------------------"
```

**Expected Output for a Working System:**

* Java Location: `/usr/bin/java`
* Java Version: `openjdk version "11..."` (or "1.8...")
* SPARK\_HOME: `/opt/spark` (or wherever they put it)
* JAVA\_HOME: `/usr/lib/jvm/java-11-openjdk-amd64`
