"""Task group for {dataset_name} pipeline."""

from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator


def create_{dataset_name}_task_group():
    """Create task group for {dataset_name} pipeline."""
    
    with TaskGroup(group_id='{dataset_name}_pipeline') as task_group:
        # Extract task
        extract_task = PythonOperator(
            task_id='extract_{dataset_name}',
            python_callable=extract_function,
        )
        
        # Transform task
        transform_task = PythonOperator(
            task_id='transform_{dataset_name}',
            python_callable=transform_function,
        )
        
        # Load task
        load_task = PythonOperator(
            task_id='load_{dataset_name}',
            python_callable=load_function,
        )
        
        # Set dependencies
        extract_task >> transform_task >> load_task
        
    return task_group