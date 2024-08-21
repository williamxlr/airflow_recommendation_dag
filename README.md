---
description: >-
  William Alexander Rodríguez Arroyo | Data Analyst | https://wa.me/573117489870
  | waradatamde@gmail.com
---

# Apache Airflow

Airflow (Apache Airflow) es una plataforma de flujo de trabajo de código abierto para programar y monitorear flujos de trabajo complejos. Un flujo de trabajo en Airflow se representa mediante un DAG

Vamos a ver un ejemplo sencillo de DAG en Airflow para entender mejor cómo funciona. Este ejemplo consiste en un DAG que tiene tres tareas simples: `tarea_1`, `tarea_2`, y `tarea_3`, donde `tarea_1` se ejecuta primero, luego `tarea_2`, y finalmente `tarea_3`.

Aquí está el código del DAG:

```python
// ptyhon

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Función que será ejecutada por las tareas PythonOperator
def print_hello():
    print('Hola mundo')

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'ejemplo_dag',
    default_args=default_args,
    description='Un DAG simple de ejemplo',
    schedule_interval='@daily',
)

# Definición de las tareas
tarea_1 = PythonOperator(
    task_id='tarea_1',
    python_callable=print_hello,
    dag=dag,
)

tarea_2 = DummyOperator(
    task_id='tarea_2',
    dag=dag,
)

tarea_3 = DummyOperator(
    task_id='tarea_3',
    dag=dag,
)

# Definición de las dependencias
tarea_1 >> tarea_2 >> tarea_3

```
