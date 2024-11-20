import time
from datetime import datetime

import schedule
from dotenv import load_dotenv
from setproctitle import setproctitle

from databases.pgdb import PgDB
from utils.mailer import send_mail
from utils.utils import get_env


def get_max_dates_of_views():
    query = """
    select 'kpi_nrcelldu_flex' as view, max(date_id) as maxd from dnb.hourly_stats.kpi_nrcelldu_flex
    UNION
    select 'kpi_nrcellcu_flex' as view, max(date_id) as maxd from dnb.hourly_stats.kpi_nrcellcu_flex
    UNION
    select 'kpi_eutrancellfdd_flex' as view, max(date_id) as maxd from dnb.hourly_stats.kpi_eutrancellfdd_flex
    UNION
    select 'kpi_eutrancellrelation_mno' as view, max(date_id) as maxd from dnb.hourly_stats.kpi_eutrancellrelation_mno;
    """

    with PgDB() as db:
        results, columns = db.query(query)
        return [{column: result[i]
                 for i, column in enumerate(columns)}
                for result in results]


def get_delay_in_hours():
    view_dates = get_max_dates_of_views()
    for view_date in view_dates:
        view_date['delay_in_hours'] = (datetime.now() - view_date['maxd']).total_seconds() / 3600
    return view_dates


def send_alert():
    view_dates = get_delay_in_hours()
    # html table with view, max date, delay in hours
    # send email
    html_header = """
    <html>
    <head>
    """
    html_body = """
    <body>
    """
    html_table = """
    <table border="1">
    <tr>
    <th>View</th>
    <th>Max Date</th>
    <th>Delay in Hours</th>
    </tr>
    """
    for view_date in view_dates:
        html_table += f"""
        <tr>
        <td>{view_date['view']}</td>
        <td>{view_date['maxd']}</td>
        <td>{view_date['delay_in_hours']}</td>
        </tr>
        """
    html_table += """
    </table>
    """
    html_footer = """
    </body>
    </html>
    """
    html_content = html_header + html_body + html_table + html_footer
    send_mail("Alert: Delay in Views",
              html_content,
              [get_env('MY_EMAIL'),
               *get_env('OTHER_EMAILS').split(',')
               ])


if __name__ == '__main__':
    load_dotenv()
    setproctitle('__check_delay_in_views__')
    schedule.every(2).hours.at(":00").do(send_alert)
    while True:
        schedule.run_pending()
        time.sleep(60 * 30)
    # nohup python -m utils.check_views > check_views.log 2>&1 &
    #  ps aux | grep -E '__.*__'
