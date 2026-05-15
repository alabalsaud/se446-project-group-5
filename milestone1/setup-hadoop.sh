#!/bin/bash
# Hadoop Installation & Configuration Script for Mac (Apple Silicon / Intel)
# Run this script after fixing Homebrew permissions if needed

set -e

echo "=========================================="
echo "  Hadoop Setup for MapReduce Project"
echo "=========================================="

# Detect paths (Homebrew on Apple Silicon uses /opt/homebrew)
if [[ -d /opt/homebrew ]]; then
    BREW_PREFIX="/opt/homebrew"
else
    BREW_PREFIX="/usr/local"
fi

# Step 1: Fix Homebrew permissions (run once if you get permission errors)
echo ""
echo "[1/6] Checking Homebrew permissions..."
if [[ ! -w "$BREW_PREFIX/Cellar" ]]; then
    echo "Homebrew directories are not writable. Run this command first (will ask for password):"
    echo "  sudo chown -R \$(whoami) $BREW_PREFIX $BREW_PREFIX/Cellar $BREW_PREFIX/opt $BREW_PREFIX/var/homebrew"
    echo ""
    read -p "Press Enter after fixing permissions, or Ctrl+C to exit..."
fi

# Step 2: Install Java and Hadoop
echo ""
echo "[2/6] Installing Java 11 and Hadoop via Homebrew..."
brew install openjdk@11 hadoop

# Set JAVA_HOME (Homebrew location - paths vary by version)
for jpath in "$BREW_PREFIX/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home" \
             "$BREW_PREFIX/opt/openjdk@11" \
             "$BREW_PREFIX/Cellar/openjdk@11/"*/libexec/openjdk.jdk/Contents/Home; do
    if [[ -d "$jpath" ]] && [[ -x "$jpath/bin/java" ]]; then
        JAVA_HOME="$jpath"
        break
    fi
done
[[ -z "$JAVA_HOME" ]] && JAVA_HOME="$BREW_PREFIX/opt/openjdk@11"
export JAVA_HOME

# Hadoop home (libexec contains the actual installation)
HADOOP_HOME="$BREW_PREFIX/opt/hadoop/libexec"
if [[ ! -d "$HADOOP_HOME" ]]; then
    HADOOP_HOME=$(brew --prefix hadoop)/libexec
fi
[[ ! -d "$HADOOP_HOME" ]] && HADOOP_HOME=$(find "$BREW_PREFIX/Cellar/hadoop" -name "libexec" -type d 2>/dev/null | head -1)

echo "JAVA_HOME=$JAVA_HOME"
echo "HADOOP_HOME=$HADOOP_HOME"

# Step 3: Copy configuration files
echo ""
echo "[3/6] Copying Hadoop configuration files..."
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_SRC="$SCRIPT_DIR/hadoop-config"
CONFIG_DEST="$HADOOP_HOME/etc/hadoop"

cp "$CONFIG_SRC/core-site.xml" "$CONFIG_DEST/"
cp "$CONFIG_SRC/hdfs-site.xml" "$CONFIG_DEST/"
cp "$CONFIG_SRC/mapred-site.xml" "$CONFIG_DEST/"
cp "$CONFIG_SRC/yarn-site.xml" "$CONFIG_DEST/"

# Set JAVA_HOME in hadoop-env.sh
if grep -q "export JAVA_HOME" "$CONFIG_DEST/hadoop-env.sh" 2>/dev/null; then
    sed -i.bak "s|.*export JAVA_HOME.*|export JAVA_HOME=$JAVA_HOME|" "$CONFIG_DEST/hadoop-env.sh"
else
    echo "export JAVA_HOME=$JAVA_HOME" >> "$CONFIG_DEST/hadoop-env.sh"
fi

# Step 4: SSH setup for localhost (required by Hadoop)
echo ""
echo "[4/6] Setting up SSH for passwordless localhost login..."
if [[ ! -f ~/.ssh/id_rsa ]]; then
    ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa -q
fi
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys 2>/dev/null || true
chmod 600 ~/.ssh/authorized_keys 2>/dev/null || true

# Step 5: Add to shell profile
echo ""
echo "[5/6] Adding environment variables to shell profile..."
SHELL_RC="$HOME/.zshrc"
[[ -f "$HOME/.bash_profile" ]] && SHELL_RC="$HOME/.bash_profile"

ENV_BLOCK='
# Hadoop (MapReduce Project)
export JAVA_HOME="'"$JAVA_HOME"'"
export HADOOP_HOME="'"$HADOOP_HOME"'"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
'

if ! grep -q "HADOOP_HOME" "$SHELL_RC" 2>/dev/null; then
    echo "$ENV_BLOCK" >> "$SHELL_RC"
    echo "Added to $SHELL_RC"
else
    echo "Hadoop env already in $SHELL_RC"
fi

# Step 6: Format HDFS (first time only) and start services
echo ""
echo "[6/6] Formatting HDFS and starting Hadoop..."
source "$SHELL_RC" 2>/dev/null || export JAVA_HOME HADOOP_HOME PATH

# Format namenode (only needed first time - will prompt if already formatted)
hdfs namenode -format -force 2>/dev/null || true

echo ""
echo "=========================================="
echo "  Setup complete! Next steps:"
echo "=========================================="
echo ""
echo "1. Open a NEW terminal (or run: source $SHELL_RC)"
echo ""
echo "2. Start Hadoop services:"
echo "   start-dfs.sh"
echo "   start-yarn.sh"
echo ""
echo "3. Create HDFS directories and upload data:"
echo "   hdfs dfs -mkdir -p /user/$(whoami)"
echo "   hdfs dfs -mkdir -p /data"
echo "   hdfs dfs -put your_data.csv /data/"
echo ""
echo "4. Run MapReduce streaming job:"
echo "   mapred streaming -files mapper.py,reducer.py -mapper \"python3 mapper.py\" -reducer \"python3 reducer.py\" -input /data/your_data.csv -output /user/$(whoami)/output"
echo ""
echo "5. View results:"
echo "   hdfs dfs -cat /user/$(whoami)/output/part-00000"
echo ""
echo "6. Stop Hadoop when done:"
echo "   stop-yarn.sh"
echo "   stop-dfs.sh"
echo ""
