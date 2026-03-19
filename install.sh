#!/bin/bash
#
# Aerosmart Gateway Installation Script
# Installs the Aerosmart Gateway and configures autostart
#
# Usage: ./install.sh [--wheezy | --systemd | --auto]
#   --wheezy   : Force SysVinit installation (for Raspbian Wheezy)
#   --systemd  : Force systemd installation (for newer systems)
#   --auto     : Auto-detect init system (default)
#   --uninstall: Remove the service
#

set -e

# Configuration
APP_DIR="/opt/aerosmart"
BINARY_NAME="aerosmart-gateway-armv6"
CONFIG_FILE="config.yaml"
REGISTERS_FILE="registers.yaml"
INIT_SCRIPT="/etc/init.d/aerosmart"
SYSTEMD_SERVICE="/lib/systemd/system/aerosmart.service"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print functions
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
check_root() {
    if [ "$EUID" -ne 0 ]; then
        print_error "This script must be run as root"
        exit 1
    fi
}

# Detect init system
detect_init_system() {
    if [ -d /run/systemd/system ]; then
        echo "systemd"
    elif [ -f /sbin/init ] && /sbin/init --version 2>/dev/null | grep -q "sysvinit"; then
        echo "sysvinit"
    else
        echo "unknown"
    fi
}

# Create application directory
create_app_directory() {
    if [ ! -d "$APP_DIR" ]; then
        print_info "Creating application directory: $APP_DIR"
        mkdir -p "$APP_DIR"
        chmod 755 "$APP_DIR"
    else
        print_info "Application directory already exists: $APP_DIR"
    fi
}

# Copy files to application directory
copy_files() {
    print_info "Copying files to $APP_DIR"
    
    # Get the directory where the script is located
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    
    # Ensure APP_DIR exists
    mkdir -p "$APP_DIR"
    
    # Copy binary (skip if already in target location)
    if [ -f "$SCRIPT_DIR/$BINARY_NAME" ]; then
        TARGET_PATH="$APP_DIR/$BINARY_NAME"
        SOURCE_PATH="$SCRIPT_DIR/$BINARY_NAME"
        if [ "$SOURCE_PATH" != "$TARGET_PATH" ]; then
            cp "$SOURCE_PATH" "$TARGET_PATH"
            chmod 755 "$TARGET_PATH"
            print_info "Binary installed: $TARGET_PATH"
        else
            print_info "Binary already in place: $TARGET_PATH"
        fi
    else
        print_error "Binary not found: $BINARY_NAME"
        exit 1
    fi
    
    # Copy config files (skip if already exists and is same)
    if [ -f "$SCRIPT_DIR/$CONFIG_FILE" ]; then
        if [ -f "$APP_DIR/$CONFIG_FILE" ]; then
            # Check if files are different
            if ! cmp -s "$SCRIPT_DIR/$CONFIG_FILE" "$APP_DIR/$CONFIG_FILE"; then
                cp "$SCRIPT_DIR/$CONFIG_FILE" "$APP_DIR/"
                print_info "Config installed: $APP_DIR/$CONFIG_FILE"
            else
                print_info "Config already in place: $APP_DIR/$CONFIG_FILE"
            fi
        else
            cp "$SCRIPT_DIR/$CONFIG_FILE" "$APP_DIR/"
            print_info "Config installed: $APP_DIR/$CONFIG_FILE"
        fi
    else
        print_warn "Config file not found: $CONFIG_FILE"
    fi
    
    if [ -f "$SCRIPT_DIR/$REGISTERS_FILE" ]; then
        if [ -f "$APP_DIR/$REGISTERS_FILE" ]; then
            if ! cmp -s "$SCRIPT_DIR/$REGISTERS_FILE" "$APP_DIR/$REGISTERS_FILE"; then
                cp "$SCRIPT_DIR/$REGISTERS_FILE" "$APP_DIR/"
                print_info "Registers file installed: $APP_DIR/$REGISTERS_FILE"
            else
                print_info "Registers file already in place: $APP_DIR/$REGISTERS_FILE"
            fi
        else
            cp "$SCRIPT_DIR/$REGISTERS_FILE" "$APP_DIR/"
            print_info "Registers file installed: $APP_DIR/$REGISTERS_FILE"
        fi
    else
        print_warn "Registers file not found: $REGISTERS_FILE"
    fi
    
    # Copy init script
    if [ -f "$SCRIPT_DIR/init-script.sh" ]; then
        TARGET_PATH="$APP_DIR/aerosmart-init.sh"
        SOURCE_PATH="$SCRIPT_DIR/init-script.sh"
        if [ "$SOURCE_PATH" != "$TARGET_PATH" ]; then
            cp "$SOURCE_PATH" "$TARGET_PATH"
            chmod 755 "$TARGET_PATH"
            print_info "Init script installed: $TARGET_PATH"
        else
            print_info "Init script already in place: $TARGET_PATH"
        fi
    fi
    
    # Copy systemd service
    if [ -f "$SCRIPT_DIR/aerosmart.service" ]; then
        TARGET_PATH="$APP_DIR/aerosmart.service"
        SOURCE_PATH="$SCRIPT_DIR/aerosmart.service"
        if [ "$SOURCE_PATH" != "$TARGET_PATH" ]; then
            cp "$SOURCE_PATH" "$TARGET_PATH"
            print_info "Systemd service installed: $TARGET_PATH"
        else
            print_info "Systemd service already in place: $TARGET_PATH"
        fi
    fi
    
    # Copy logrotate config
    if [ -f "$SCRIPT_DIR/logrotate.conf" ]; then
        cp "$SCRIPT_DIR/logrotate.conf" "$APP_DIR/"
        print_info "Logrotate config installed: $APP_DIR/logrotate.conf"
    fi
}

# Install logrotate configuration
install_logrotate() {
    print_info "Installing logrotate configuration..."
    
    # Check if logrotate is installed
    if ! command -v logrotate &> /dev/null; then
        print_warn "logrotate not installed, skipping logrotate configuration"
        print_info "To install logrotate: sudo apt-get install logrotate"
        return 0
    fi
    
    # Copy logrotate config
    if [ -f "$APP_DIR/logrotate.conf" ]; then
        cp "$APP_DIR/logrotate.conf" "/etc/logrotate.d/aerosmart"
        chmod 644 "/etc/logrotate.d/aerosmart"
        print_info "Logrotate config installed: /etc/logrotate.d/aerosmart"
    else
        print_warn "Logrotate config not found"
    fi
}

# Install SysVinit script
install_sysvinit() {
    print_info "Installing SysVinit script..."
    
    # Copy init script to /etc/init.d
    if [ -f "$APP_DIR/aerosmart-init.sh" ]; then
        cp "$APP_DIR/aerosmart-init.sh" "$INIT_SCRIPT"
        chmod 755 "$INIT_SCRIPT"
        
        # Configure runlevels
        update-rc.d aerosmart defaults 2>/dev/null || true
        
        print_info "SysVinit script installed to: $INIT_SCRIPT"
        print_info "Runlevels configured for automatic start"
    else
        print_error "Init script not found"
        exit 1
    fi
}

# Uninstall SysVinit script
uninstall_sysvinit() {
    print_info "Uninstalling SysVinit script..."
    
    # Stop if running
    if [ -f "$INIT_SCRIPT" ]; then
        $INIT_SCRIPT stop 2>/dev/null || true
        
        # Remove runlevels
        update-rc.d aerosmart remove 2>/dev/null || true
        
        # Remove init script
        rm -f "$INIT_SCRIPT"
        
        print_info "SysVinit script removed"
    fi
}

# Install systemd service
install_systemd() {
    print_info "Installing systemd service..."
    
    # Copy service file
    if [ -f "$APP_DIR/aerosmart.service" ]; then
        cp "$APP_DIR/aerosmart.service" "$SYSTEMD_SERVICE"
        chmod 644 "$SYSTEMD_SERVICE"
        
        # Reload systemd
        systemctl daemon-reload
        
        # Enable service
        systemctl enable aerosmart.service
        
        print_info "Systemd service installed to: $SYSTEMD_SERVICE"
        print_info "Service enabled for automatic start"
    else
        print_error "Systemd service file not found"
        exit 1
    fi
}

# Uninstall systemd service
uninstall_systemd() {
    print_info "Uninstalling systemd service..."
    
    # Stop if running
    systemctl stop aerosmart.service 2>/dev/null || true
    
    # Disable service
    systemctl disable aerosmart.service 2>/dev/null || true
    
    # Remove service file
    rm -f "$SYSTEMD_SERVICE"
    
    # Reload systemd
    systemctl daemon-reload
    
    print_info "Systemd service removed"
}

# Start the service
start_service() {
    local init_system="$1"
    
    print_info "Starting Aerosmart Gateway..."
    
    if [ "$init_system" = "systemd" ]; then
        systemctl start aerosmart.service
    elif [ "$init_system" = "sysvinit" ]; then
        $INIT_SCRIPT start
    else
        print_error "Unknown init system"
        exit 1
    fi
}

# Stop the service
stop_service() {
    local init_system="$1"
    
    print_info "Stopping Aerosmart Gateway..."
    
    if [ "$init_system" = "systemd" ]; then
        systemctl stop aerosmart.service 2>/dev/null || true
    elif [ "$init_system" = "sysvinit" ]; then
        $INIT_SCRIPT stop 2>/dev/null || true
    fi
}

# Show status
show_status() {
    local init_system="$1"
    
    if [ "$init_system" = "systemd" ]; then
        systemctl status aerosmart.service
    elif [ "$init_system" = "sysvinit" ]; then
        $INIT_SCRIPT status
    else
        print_error "Unknown init system"
        exit 1
    fi
}

# Main installation function
install() {
    local mode="$1"
    local init_system
    
    # Detect or use specified init system
    if [ "$mode" = "wheezy" ]; then
        init_system="sysvinit"
    elif [ "$mode" = "systemd" ]; then
        init_system="systemd"
    else
        init_system=$(detect_init_system)
    fi
    
    print_info "Detected init system: $init_system"
    
    # Create app directory
    create_app_directory
    
    # Copy files
    copy_files
    
    # Install logrotate configuration
    install_logrotate
    
    # Install based on init system
    if [ "$init_system" = "systemd" ]; then
        install_systemd
    elif [ "$init_system" = "sysvinit" ]; then
        install_sysvinit
    else
        print_error "Could not detect init system. Please specify --wheezy or --systemd"
        exit 1
    fi
    
    print_info "Installation complete!"
    print_info ""
    print_info "To start the service:"
    if [ "$init_system" = "systemd" ]; then
        echo "  sudo systemctl start aerosmart"
    else
        echo "  sudo $INIT_SCRIPT start"
    fi
    print_info ""
    print_info "To check status:"
    if [ "$init_system" = "systemd" ]; then
        echo "  sudo systemctl status aerosmart"
    else
        echo "  sudo $INIT_SCRIPT status"
    fi
}

# Main uninstallation function
uninstall() {
    local mode="$1"
    local init_system
    
    # Detect or use specified init system
    if [ "$mode" = "wheezy" ]; then
        init_system="sysvinit"
    elif [ "$mode" = "systemd" ]; then
        init_system="systemd"
    else
        init_system=$(detect_init_system)
    fi
    
    print_info "Detected init system: $init_system"
    
    # Uninstall based on init system
    if [ "$init_system" = "systemd" ]; then
        uninstall_systemd
    elif [ "$init_system" = "sysvinit" ]; then
        uninstall_sysvinit
    else
        print_error "Could not detect init system. Please specify --wheezy or --systemd"
        exit 1
    fi
    
    # Optionally remove application files
    # Uncomment the following lines to remove application files
    # print_info "Removing application files..."
    # rm -rf "$APP_DIR"
    
    print_info "Uninstallation complete!"
}

# Show usage
usage() {
    echo "Aerosmart Gateway Installation Script"
    echo ""
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --wheezy    Install SysVinit script (for Raspbian Wheezy)"
    echo "  --systemd   Install systemd service (for newer systems)"
    echo "  --auto      Auto-detect and install (default)"
    echo "  --uninstall Remove the service"
    echo "  --start     Start the service after installation"
    echo "  --stop      Stop the service"
    echo "  --status    Show service status"
    echo "  -h, --help  Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --auto              # Auto-detect and install"
    echo "  $0 --systemd --start  # Install systemd and start"
    echo "  $0 --uninstall        # Remove the service"
}

# Main script
main() {
    local mode="auto"
    local action=""
    
    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            --wheezy)
                mode="wheezy"
                ;;
            --systemd)
                mode="systemd"
                ;;
            --auto)
                mode="auto"
                ;;
            --uninstall)
                action="uninstall"
                ;;
            --start)
                action="start"
                ;;
            --stop)
                action="stop"
                ;;
            --status)
                action="status"
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
        shift
    done
    
    # Check root for most operations
    if [ "$action" = "install" ] || [ "$action" = "uninstall" ] || [ "$action" = "start" ] || [ "$action" = "stop" ] || [ "$action" = "status" ]; then
        check_root
    fi
    
    # Detect init system for status/start/stop actions
    if [ "$action" = "start" ] || [ "$action" = "stop" ] || [ "$action" = "status" ]; then
        local init_system=$(detect_init_system)
        
        case "$action" in
            start)
                start_service "$init_system"
                ;;
            stop)
                stop_service "$init_system"
                ;;
            status)
                show_status "$init_system"
                ;;
        esac
        exit 0
    fi
    
    # Install or uninstall
    if [ "$action" = "uninstall" ]; then
        uninstall "$mode"
    else
        install "$mode"
    fi
}

# Run main function
main "$@"
