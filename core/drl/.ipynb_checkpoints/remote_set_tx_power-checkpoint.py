import pexpect
import logging

logging.basicConfig(level=logging.INFO)

def remote_set_tx_power(ip, user, password, power_val, cli_path='~/nrc_pkg/script/cli_app'):

    remote_cmd = f"{cli_path} set txpwr fixed {power_val}"
    ssh_cmd = f"ssh {user}@{ip} '{remote_cmd}'"

    logging.info(f"Connecting to {ip}...")

    try:
        # Spawn the ssh process
        child = pexpect.spawn(ssh_cmd, encoding='utf-8', timeout=10)

        # 0: Password prompt
        # 1: "Are you sure..." (fingerprint check for new hosts)
        # 2: EOF (Process finished without asking for password - e.g. if keys were setup)
        index = child.expect(['[Pp]assword:', 'continue connecting', pexpect.EOF])

        if index == 1:
            # Handle the "Are you sure you want to continue connecting (yes/no)?" prompt
            child.sendline('yes')
            # Wait for the password prompt again
            child.expect('[Pp]assword:')
            child.sendline(password)
        
        elif index == 0:
            # Matched 'Password:' directly
            child.sendline(password)
        
        elif index == 2:
            # No password needed 
            pass

        # Wait for the command to finish and get the output
        output = child.read()
        child.close()

        # Check exit status (0 means success)
        if child.exitstatus == 0:
            logging.info(f"Success: TX power set to {power_val}")
            return True
        else:
            logging.error(f"Command finished with error code {child.exitstatus}")
            logging.error(f"Output: {output.strip()}")
            return False

    except pexpect.TIMEOUT:
        logging.error("Error: Connection timed out.")
        return False
    except Exception as e:
        logging.error(f"Failed: {e}")
        return False