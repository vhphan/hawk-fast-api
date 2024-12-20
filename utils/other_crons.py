import subprocess
import time

import schedule
from loguru import logger


def run_script(script_path):
    logger.info(f"Running script: {script_path}")
    subprocess.Popen([script_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info(f"Finished running script: {script_path}")


def main():
    # HAWK Inserter Hourly
    schedule.every().hour.at(":40").do(run_script, "/home/hawkuser/Desktop/tests/Hourly/NR/run_insert_hourly_nr.sh")
    schedule.every().hour.at(":45").do(run_script, "/home/hawkuser/Desktop/tests/LTE/hourly/run_insert_hourly_lte.sh")

    # HAWK Inserter
    schedule.every().day.at("06:45").do(run_script, "/home/hawkuser/Desktop/tests/insert_data/run_insert.sh")
    schedule.every().day.at("06:45").do(run_script, "/home/hawkuser/Desktop/tests/LTE/daily/run_insert_daily_lte.sh")

    # SA daily
    schedule.every().day.at("06:40").do(run_script, "/home/hawkuser/Desktop/tests/sa_kpis/daily/run_insert_sa_daily.sh")
    # SA hourly
    schedule.every().hour.at(":40").do(run_script,
                                       "/home/hawkuser/Desktop/tests/sa_kpis/hourly/run_insert_sa_hourly.sh")

    # Traffic early warning
    schedule.every().hour.at(":57").do(run_script,
                                       "/data1/HAWK_bots/traffic_early_warning/run_traffic_early_warning.sh")
    schedule.every().hour.at(":57").do(run_script,
                                       "/data1/HAWK_bots/traffic_early_warning/plmn_level/run_traffic_early_warning_plmn.sh")

    schedule.run_all()
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == '__main__':
    main()
