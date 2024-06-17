---
description: >-
  Explicación del código, pasos, tareas y funciones clave para el entendimiento
  del ejemplo básico del script DAG en lenguaje PYTHON
---

# Explicación del Código



* **Importaciones**:
  * `DAG` de `airflow`: Para definir el DAG.
  * `DummyOperator` de `airflow.operators.dummy_operator`: Un operador que no hace nada, útil para pruebas y estructura del flujo.
  * `PythonOperator` de `airflow.operators.python_operator`: Permite ejecutar una función Python como una tarea.
  * `datetime` de `datetime`: Para manejar fechas y horas.
* **Función de la tarea**:
  * `print_hello`: Una función simple que imprime "Hola mundo". Esta función será ejecutada por `PythonOperator`.
* **Definición del DAG**:
  * `default_args`: Parámetros por defecto para las tareas del DAG, como el propietario, fecha de inicio y número de reintentos.
  * `dag`: Instancia del DAG con un nombre (`'ejemplo_dag'`), los argumentos por defecto (`default_args`), una descripción y un intervalo de programación (`'@daily'`, que significa que se ejecutará diariamente).
* **Definición de las tareas**:
  * `tarea_1`: Una instancia de `PythonOperator` que ejecuta la función `print_hello`.
  * `tarea_2` y `tarea_3`: Instancias de `DummyOperator`.
* **Definición de las dependencias**:
  * `tarea_1 >> tarea_2 >> tarea_3`: Esto define que `tarea_1` debe ejecutarse antes de `tarea_2`, y `tarea_2` debe ejecutarse antes de `tarea_3`.
