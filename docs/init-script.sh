#!/bin/sh
### BEGIN INIT INFO
# Provides:          aerosmart
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Aerosmart Gateway daemon
# Description:       Aerosmart Gateway - connects Aerosmart ventilation to MQTT
### END INIT INFO

# Aerosmart Gateway SysVinit Init Script
# For Raspbian Wheezy and other SysVinit-based systems
# Location: /etc/init.d/aerosmart

NAME=aerosmart
DESC="Aerosmart Gateway"
PROGRAM=/opt/aerosmart/aerosmart-gateway
PIDFILE=/var/run/aerosmart.pid
LOGFILE=/var/log/aerosmart.log
CONFIG=/opt/aerosmart/config.yaml
REGISTERS=/opt/aerosmart/registers.yaml

# Check if file logging is disabled in config
# Returns "true" if file logging should be disabled
check_file_logging_disabled() {
    if [ -f "$CONFIG" ]; then
        # Check if file_logging is set to false
        if grep -q "file_logging: false" "$CONFIG" 2>/dev/null; then
            echo "true"
            return
        fi
        # Check if log_file is empty or set to "-"
        if grep -q 'log_file: ""' "$CONFIG" 2>/dev/null || grep -q 'log_file: "-"' "$CONFIG" 2>/dev/null; then
            echo "true"
            return
        fi
    fi
    echo "false"
}

# Export display for Home Assistant GUI if needed
# export DISPLAY=:0

# Check if binary exists
if [ ! -x "$PROGRAM" ]; then
    echo "Error: $PROGRAM not found or not executable"
    exit 1
fi

# Function to start the daemon
do_start() {
    echo "Starting $DESC: $NAME"
    
    # Check if already running
    if [ -f "$PIDFILE" ]; then
        PID=$(cat $PIDFILE 2>/dev/null)
        if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
            echo "$NAME is already running (PID: $PID)"
            return 1
        fi
        # Clean up stale PID file
        rm -f $PIDFILE
    fi
    
    # Check if file logging is disabled
    FILE_LOGGING_DISABLED=$(check_file_logging_disabled)
    
    # Build command arguments
    CMD_ARGS="-config $CONFIG -registers $REGISTERS"
    
    # Start the daemon with start-stop-daemon
    # --make-pidfile: automatically create PID file
    # --background: run in background
    # --no-close: don't close file descriptors
    if [ "$FILE_LOGGING_DISABLED" = "true" ]; then
        # Run without file logging (stdout only)
        start-stop-daemon -S \
            --pidfile "$PIDFILE" \
            --make-pidfile \
            --background \
            --no-close \
            --startas "$PROGRAM" \
            -- \
            $CMD_ARGS
    else
        # Run with file logging
        start-stop-daemon -S \
            --pidfile "$PIDFILE" \
            --make-pidfile \
            --background \
            --no-close \
            --startas "$PROGRAM" \
            -- \
            $CMD_ARGS \
            >> "$LOGFILE" 2>&1
    fi
    
    # Wait a moment and check if started
    sleep 2
    
    if [ -f "$PIDFILE" ]; then
        PID=$(cat $PIDFILE 2>/dev/null)
        if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
            echo "$NAME started successfully (PID: $PID)"
            return 0
        fi
    fi
    
    echo "Failed to start $NAME"
    return 1
}

# Function to stop the daemon
do_stop() {
    echo "Stopping $DESC: $NAME"
    
    if [ ! -f "$PIDFILE" ]; then
        echo "$NAME is not running (no PID file)"
        return 0
    fi
    
    PID=$(cat $PIDFILE 2>/dev/null)
    
    # Try graceful shutdown first
    if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
        # Send SIGTERM for graceful shutdown
        kill -TERM $PID 2>/dev/null
        
        # Wait for graceful shutdown (max 10 seconds)
        for i in 1 2 3 4 5 6 7 8 9 10; do
            if ! kill -0 $PID 2>/dev/null; then
                echo "$NAME stopped gracefully"
                rm -f $PIDFILE
                return 0
            fi
            sleep 1
        done
        
        # Force kill if still running
        echo "Graceful shutdown timed out, forcing..."
        kill -KILL $PID 2>/dev/null
        sleep 1
    fi
    
    # Final cleanup
    if [ -n "$PID" ] && kill -0 $PID 2>/dev/null; then
        echo "Failed to stop $NAME"
        return 1
    fi
    
    rm -f $PIDFILE
    echo "$NAME stopped"
    return 0
}

# Function to restart the daemon
do_restart() {
    echo "Restarting $DESC: $NAME"
    do_stop
    sleep 2
    do_start
}

# Function to check status
do_status() {
    if [ ! -f "$PIDFILE" ]; then
        echo "$NAME is not running (no PID file)"
        return 3
    fi
    
    PID=$(cat $PIDFILE 2>/dev/null)
    
    if [ -z "$PID" ]; then
        echo "$NAME is not running (empty PID file)"
        return 3
    fi
    
    if kill -0 $PID 2>/dev/null; then
        echo "$NAME is running (PID: $PID)"
        return 0
    else
        echo "$NAME is not running (stale PID file)"
        rm -f $PIDFILE
        return 3
    fi
}

# Function to force restart (crash recovery)
do_force_reload() {
    echo "Force reloading $DESC: $NAME (crash recovery)"
    do_stop
    sleep 2
    do_start
}

# Main case statement
case "$1" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_restart
        ;;
    force-reload)
        do_force_reload
        ;;
    status)
        do_status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|force-reload|status}"
        exit 1
        ;;
esac

exit $?
