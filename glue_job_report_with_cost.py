import ast
import os
import boto3
from collections import OrderedDict
import dateutil.tz
from datetime import datetime, timedelta, date
from pprint import pprint

session = boto3.Session()
glue_client = session.client('glue')
cost_explorer_client = boto3.client('ce')

runs = []
J_N = "Job Name"
JR = "JobRuns"
JN = "JobName"
SO = "StartedOn"
JS = "Job State"
JRS = "JobRunState"
AT = "Attempt"
CO = "CompletedOn"
ET = "Execution Time"
DPU = "Capacity/Workers"
COST = "Job Run Cost($)"
GLUE_JOB_COST_PER_DPU = 0.44

table_html = """'<!DOCTYPE html> <html> <head> <style> table { font-family: arial, sans-serif; border-collapse: collapse; width: 100%; } h2 { text-align: center; } h3 { text-align: center; } td, th { border: 1px solid #dceccf; text-align: left; padding: 8px; } tr:nth-child(even) { background-color: #dceccf; } </style> </head> <body> <h2> Glue Job Status (Past 24 Hours) </h2> <h3>'"""
table_header = """</h3> <table> <tr> <th>Job Name</th> <th>Job State</th> <th>Attempt</th> <th>Started On</th> <th>Completed On</th> <th>Execution Time(Secs)</th> <th>Capacity/Workers</th><th>Job Run Cost($)</th></tr>"""


def get_prev_day():
    return (date.today() - timedelta(days=1)).isoformat()


def get_today():
    return date.today().isoformat()


def get_total_billing(client) -> dict:
    start_date = get_prev_day()
    end_date = get_today()
    print(start_date, end_date)

    response = client.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Filter={'Dimensions': {'Key': 'SERVICE', 'Values': ['AWS Glue']}},
        Granularity='DAILY',
        Metrics=[
            'NetUnblendedCost'
        ]
    )
    return {
        'start': response['ResultsByTime'][0]['TimePeriod']['Start'],
        'end': response['ResultsByTime'][0]['TimePeriod']['End'],
        'billing': round(float(response['ResultsByTime'][0]['Total']['NetUnblendedCost']['Amount'])),
    }


def get_jb_rn_dtl(jobName, lbh=24):
    data = glue_client.get_job_runs(JobName=jobName, MaxResults=30)
    job_runs = []
    run_count = len(data[JR])
    if run_count > 0:
        for i in range(0, min(26, run_count)):
            run = {}
            pprint(data[JR][i])
            run[JN] = data[JR][i][JN]
            try:
                if (datetime.now(dateutil.tz.tzlocal()) - data[JR][i][
                    SO]).total_seconds() > int(lbh) * 60 * 60:
                    break
            except IndexError:
                continue
            run[SO] = data[JR][i][SO]
            run[JRS] = data[JR][i][JRS]
            run['Attempt'] = data[JR][i][AT]
            if data[JR][i]['GlueVersion'] == '1.0':
                run[DPU] = data[JR][i]['MaxCapacity']
            else:
                run[DPU] = data[JR][i]['NumberOfWorkers']
            try:
                run[CO] = data[JR][i][CO]
                run[ET] = (run[CO] - run[SO]).seconds
            except KeyError:
                pass
            job_runs.append(run)
    print(job_runs)
    return job_runs


def get_job_rundetail(job_names, lbh):
    for job_name in job_names:
        run = get_jb_rn_dtl(job_name, lbh)
        runs.append(run)
    return runs


def get_job_names():
    job_names = glue_client.list_jobs(MaxResults=100)
    return job_names.get('JobNames')

def get_run_cost(number_of_dpu, execution_time):
    execution_time_in_minutes, seconds = divmod(int(execution_time),60)
    if seconds > 0:
        execution_time_in_minutes = execution_time_in_minutes + 1
    job_run_cost = round(execution_time_in_minutes * number_of_dpu * GLUE_JOB_COST_PER_DPU,2)
    return job_run_cost

def publish_in_ses(all_runs, table_html, table_header):
    lines = []
    for runs in all_runs:
        for run in runs:
            item = OrderedDict()
            item[J_N] = run.get(JN, "")
            item[JS] = run.get(JRS, "")
            item[AT] = run.get(AT, "")
            started_on = str(run.get(SO, "")).split('.')
            item[SO] = started_on[0]
            completed_on = str(run.get(CO, "")).split('.')
            item[CO] = completed_on[0]
            execution_time = str(run.get(ET, ""))
            item[ET] = execution_time
            item[DPU] = run.get(DPU, "")
            item[COST] = get_run_cost(item[DPU], execution_time)
            if len(run) != 0:
                lines.append(item)
    html_table = []
    lines = sorted(lines,
                   key=lambda i: (i[JS], i[SO], i[CO]))
    for line in lines:
        print(line.keys())
        html_table.append("<tr>")
        for key in [J_N, JS, AT, SO, CO, 'Execution Time', DPU,COST]:
            html_table.append(f"<td>{line[key]}</td>")
        html_table.append("</tr>")
    html_table.append("</table></body></html>")
    total_job = len(set(i['Job Name'] for i in lines))
    count_fail = sum(1 for i in lines if i[JS] == "FAILED")
    count_running = sum(1 for i in lines if i[JS] == "RUNNING")
    count_success = sum(1 for i in lines if i[JS] == "SUCCEEDED")
    count_all = count_fail + count_running + count_success
    aws_glue_cost = get_total_billing(cost_explorer_client)
    subject1 = f"Number of Jobs {total_job}: Executions {count_all}: Failed {count_fail} Running {count_running} Succeeded {count_success} "
    subject2 = f"AWS Glue Cost: ${aws_glue_cost['billing']} Billing Start Date: {aws_glue_cost['start']} Billing End Date: {aws_glue_cost['end']}"
    table_html_meta = table_html + subject1 + "</br>" + subject2 + table_header
    full_html = table_html_meta + "".join(html_table)
    print(full_html)
    return subject1, full_html


def main(snd, to_a, lbh, table_html, table_header):
    job_names = get_job_names()
    runs = get_job_rundetail(job_names, lbh)
    subject, html_content = publish_in_ses(runs, table_html, table_header)
    ses = boto3.client('ses')
    response = ses.send_email(
        Source=snd,
        Destination={
            'ToAddresses': ast.literal_eval(to_a)
        },
        Message={
            'Subject': {
                'Data': subject,
            },
            'Body': {
                'Html': {
                    'Data': html_content,
                }
            }
        }
    )
    print(response)


def handler(event, context):
    snd = os.environ['fromEmail']
    to_a = os.environ['toEmail']
    look_back_hours = os.environ['lookBackHours']
    main(snd, to_a, look_back_hours, table_html, table_header)
