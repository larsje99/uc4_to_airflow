# All the potential outcomes for exception handling
ALL_SUCCESS = 'all_success'
ALL_FAILED = 'all_failed'
ALL_DONE = 'all_done'
ONE_SUCCESS = 'one_success'
ONE_FAILED = 'one_failed'
DUMMY = 'dummy'

from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta, date
from scripts_uc4.schedule_converter import ScheduleConverter
from scripts_uc4.parse_xml import ParseXML
import pendulum

# FILL SCHEDULE AND JOB IN RIGHT HERE
plan_file = "UC4_MASTER_SKIPI (1)"
plan_name = "P.HDPAWS.MASTER_SKIPI.D.90000"
schedule_file_name = "UC4_JSCH_HDPAWS_2"

with DAG(
    plan_name,
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["lars.nolf@telenet.be"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "queue": ParseXML.GetQueueName(plan_file)
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="FILL IN DESCRIPTION HERE",
    schedule_interval=ScheduleConverter(schedule_file_name, plan_name),
    start_date=pendulum.datetime(2023, 1, 1, tz="Europe/Brussels"),
    catchup=False,
    tags=["uc4_test"],
):
    tasks_dict = {}
    # LOOP FOR MAKING TASKS
    for i, task in enumerate(ParseXML.PlaceJobsInList(plan_file, plan_name), start=1):
        make_task = BashOperator(
                task_id=task,
                bash_command=f'echo "{ParseXML.GetBashCommand(plan_file, task)}"')
        tasks_dict[i] = make_task

    # LOOP FOR SETTING DEPENDENCIES
    for task in tasks_dict.values():
        dependencies = ParseXML.GetDependencies(plan_file, task.task_id)

        for dependency in dependencies:
            if str(task.task_id) != str(tasks_dict[int(dependency)].task_id):
                task.set_upstream(tasks_dict[int(dependency)])
                print(f"{tasks_dict[int(dependency)].task_id} >> {task.task_id}")