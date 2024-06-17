from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# Función para extraer datos simulados
def extract_data():
    users = {'user_id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie'], 'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']}
    interactions = {'user_id': [1, 1, 2, 3, 3], 'product_id': [101, 102, 101, 103, 104]}
    products = {'product_id': [101, 102, 103, 104, 105, 106], 'product_name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E', 'Product F']}
    
    users_df = pd.DataFrame(users)
    interactions_df = pd.DataFrame(interactions)
    products_df = pd.DataFrame(products)
    
    # Directorio donde se guardarán los archivos
    output_dir = '/opt/airflow/dags/recommendations/'
    
    # Crear el directorio si no existe
    os.makedirs(output_dir, exist_ok=True)
    
    users_df.to_csv(os.path.join(output_dir, 'users.csv'), index=False)
    interactions_df.to_csv(os.path.join(output_dir, 'interactions.csv'), index=False)
    products_df.to_csv(os.path.join(output_dir, 'products.csv'), index=False)

# Función para procesar datos y generar recomendaciones
def process_data():
    input_dir = '/opt/airflow/dags/recommendations/'
    output_dir = '/opt/airflow/dags/recommendations/'
    
    users_df = pd.read_csv(os.path.join(input_dir, 'users.csv'))
    interactions_df = pd.read_csv(os.path.join(input_dir, 'interactions.csv'))
    products_df = pd.read_csv(os.path.join(input_dir, 'products.csv'))
    
    all_products = set(products_df['product_id'])
    
    # Generar recomendaciones de productos nuevos que no han sido comprados por el usuario
    recommendations = []
    for user_id in users_df['user_id']:
        purchased_products = set(interactions_df[interactions_df['user_id'] == user_id]['product_id'])
        new_recommendations = list(all_products - purchased_products)
        recommendations.append({'user_id': user_id, 'recommended_products': new_recommendations})
    
    recommendations_df = pd.DataFrame(recommendations)
    recommendations_df.to_csv(os.path.join(output_dir, 'recommendations.csv'), index=False)

# Función para guardar recomendaciones
def save_recommendations():
    input_dir = '/opt/airflow/dags/recommendations/'
    output_dir = '/opt/airflow/dags/recommendations/'
    
    recommendations_df = pd.read_csv(os.path.join(input_dir, 'recommendations.csv'))
    users_df = pd.read_csv(os.path.join(input_dir, 'users.csv'))
    
    recommendations = recommendations_df.merge(users_df, on='user_id')
    recommendations['recommended_products'] = recommendations['recommended_products'].apply(lambda x: ', '.join(map(str, eval(x))))
    recommendations.to_csv(os.path.join(output_dir, 'recommendations_with_emails.csv'), index=False)
    print(recommendations)

# Función para simular el envío de correos electrónicos con las recomendaciones
def send_email():
    input_dir = '/opt/airflow/dags/recommendations/'
    
    recommendations_df = pd.read_csv(os.path.join(input_dir, 'recommendations_with_emails.csv'))
    
    for index, row in recommendations_df.iterrows():
        user_email = row['email']
        recommended_products = row['recommended_products']
        
        print(f"Sending email to {user_email} with recommendations: {recommended_products}")

# Definimos los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definimos el DAG
dag = DAG(
    'recommendation_dag',
    default_args=default_args,
    description='Un DAG de ejemplo para recomendar productos nuevos que no han sido comprados',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 6, 14),
    catchup=False,
)

# Definimos las tareas
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_recommendations',
    python_callable=save_recommendations,
    dag=dag,
)

email_task = PythonOperator(
    task_id='send_email',
    python_callable=send_email,
    dag=dag,
)

# Definimos las dependencias entre las tareas
extract_task >> process_task >> save_task >> email_task
