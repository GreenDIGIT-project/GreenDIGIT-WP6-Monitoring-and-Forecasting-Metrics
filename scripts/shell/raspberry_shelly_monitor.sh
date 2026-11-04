#!/bin/bash

# ================================
# Raspberry Pi → Shelly IP mapping
# ================================
declare -A raspberry_to_shelly=(
  ["10.64.44.196"]="10.64.44.220"
  ["10.64.44.70"]="10.64.44.221"
  ["10.64.44.71"]="10.64.44.222"
  ["10.64.44.72"]="10.64.44.223"
  ["10.64.44.73"]="10.64.44.224"
  ["10.64.44.74"]="10.64.44.225"
  ["10.64.44.75"]="10.64.44.226"
  ["10.64.44.190"]="10.64.44.227"
  ["10.64.44.191"]="10.64.44.228"
  ["10.64.44.192"]="10.64.44.229"
  ["10.64.44.193"]="10.64.44.230"
  ["10.64.44.194"]="10.64.44.231"
  ["10.64.44.195"]="10.64.44.232"
)

# Tuning
INTERVAL=120                     # seconds between checks
PING_COUNT=1
PING_TIMEOUT=2                   # seconds
CURL_TIMEOUT=5                   # seconds
REBOOT_DELAY=5                   # seconds to wait while Shelly is OFF

# Optional: basic auth
# AUTH="user:pass"
# CURL_AUTH_OPT=( -u "$AUTH" )
CURL_AUTH_OPT=()

# ================================
# Caches
# ================================
declare -A shelly_gen_map        # ip -> 1|2|3
declare -A shelly_switch_id_map  # ip -> integer

# ================================
# Helper: Detect Shelly generation via HTTP
# ================================
detect_shelly_gen() {
  local ip="$1" resp

  # Try RPC device info (Gen2/3)
  resp=$(curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/rpc/Shelly.GetDeviceInfo" || true)
  if [[ -n "$resp" ]]; then
    if [[ "$resp" =~ \"gen\"[[:space:]]*:[[:space:]]*([0-9]+) ]]; then
      echo "${BASH_REMATCH[1]}"
      return 0
    fi
  fi

  # Gen1 exposes /shelly
  resp=$(curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/shelly" || true)
  if [[ -n "$resp" ]]; then
    echo "1"
    return 0
  fi

  return 1
}

# ================================
# Helper: Find a working Switch id for Gen2/Gen3
# ================================
find_switch_id() {
  local ip="$1" resp id

  resp=$(curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/rpc/Switch.List" || true)
  if [[ -n "$resp" ]]; then
    if [[ "$resp" =~ \"id\"[[:space:]]*:[[:space:]]*([0-9]+) ]]; then
      echo "${BASH_REMATCH[1]}"
      return 0
    fi
  fi

  for id in 0 1 2 3; do
    resp=$(curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/rpc/Switch.GetStatus?id=$id" || true)
    if [[ "$resp" =~ \"output\"[[:space:]]*:[[:space:]]*(true|false) ]]; then
      echo "$id"
      return 0
    fi
  done
  return 1
}

# ================================
# Helper: Get Shelly ON/OFF status
# ================================
get_shelly_status() {
  local ip="$1" gen="$2" resp id

  if [[ "$gen" == "2" || "$gen" == "3" ]]; then
    id="${shelly_switch_id_map[$ip]}"
    if [[ -z "$id" ]]; then
      if id=$(find_switch_id "$ip"); then
        shelly_switch_id_map[$ip]="$id"
      else
        echo "unknown"; return 0
      fi
    fi
    resp=$(curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/rpc/Switch.GetStatus?id=$id" || true)
    if [[ "$resp" =~ \"output\"[[:space:]]*:[[:space:]]*true ]]; then
      echo "on"
    elif [[ "$resp" =~ \"output\"[[:space:]]*:[[:space:]]*false ]]; then
      echo "off"
    else
      echo "unknown"
    fi
    return 0
  fi

  # Gen1
  resp=$(curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/relay/0" || true)
  if [[ "$resp" =~ \"ison\"[[:space:]]*:[[:space:]]*true ]]; then
    echo "on"
  elif [[ "$resp" =~ \"ison\"[[:space:]]*:[[:space:]]*false ]]; then
    echo "off"
  else
    echo "unknown"
  fi
}

# ================================
# Helper: Power Control
# ================================
turn_shelly_on() {
  local ip="$1" gen="$2" id
  if [[ "$gen" == "2" || "$gen" == "3" ]]; then
    id="${shelly_switch_id_map[$ip]}"
    curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/rpc/Switch.Set?id=${id:-0}&on=true" >/dev/null
  else
    curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/relay/0?turn=on" >/dev/null
  fi
}

turn_shelly_off() {
  local ip="$1" gen="$2" id
  if [[ "$gen" == "2" || "$gen" == "3" ]]; then
    id="${shelly_switch_id_map[$ip]}"
    curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/rpc/Switch.Set?id=${id:-0}&on=false" >/dev/null
  else
    curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$ip/relay/0?turn=off" >/dev/null
  fi
}

# ================================
# Main loop
# ================================
while true; do
  echo "=== Checking Raspberry Pis at $(date) ==="
  for raspberry_ip in "${!raspberry_to_shelly[@]}"; do
    shelly_ip=${raspberry_to_shelly[$raspberry_ip]}

    echo -n "Pinging Raspberry Pi $raspberry_ip... "
    if ping -c "$PING_COUNT" -W "$PING_TIMEOUT" "$raspberry_ip" &>/dev/null; then
      echo "Online ✅"
      continue
    fi

    echo "Offline ❌"
    echo -n "Checking Shelly $shelly_ip... "

    # Attempt HTTP check as fallback if Ping fails
    gen="${shelly_gen_map[$shelly_ip]}"
    if [[ -z "$gen" ]]; then
      if gen=$(detect_shelly_gen "$shelly_ip"); then
        shelly_gen_map[$shelly_ip]="$gen"
        echo "Reachable via HTTP ✅ (Gen $gen)"
      else
        echo "Unreachable via Ping and HTTP ❌ — skipping."
        continue
      fi
    else
      # Quick HTTP verify for cached Gen
      if ! curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$shelly_ip/shelly" &>/dev/null && \
         ! curl -s --connect-timeout "$CURL_TIMEOUT" "${CURL_AUTH_OPT[@]}" "http://$shelly_ip/rpc/Shelly.GetDeviceInfo" &>/dev/null; then
         echo "Unreachable via HTTP ❌ — skipping."
         continue
      fi
      echo "Reachable via HTTP ✅"
    fi

    status=$(get_shelly_status "$shelly_ip" "$gen")
    case "$status" in
      on)
        echo "Pi is down but Shelly is ON. Toggling power to reboot Pi..."
        turn_shelly_off "$shelly_ip" "$gen"
        sleep "$REBOOT_DELAY"
        turn_shelly_on "$shelly_ip" "$gen"
        echo "Reboot command sequence complete ⚡"
        ;;
      off)
        echo "Shelly is OFF — turning ON to boot Pi."
        turn_shelly_on "$shelly_ip" "$gen"
        ;;
      *)
        echo "Status unknown — forcing ON to attempt recovery."
        turn_shelly_on "$shelly_ip" "$gen"
        ;;
    esac
  done

  echo "Sleeping for $INTERVAL seconds..."
  sleep "$INTERVAL"
done
