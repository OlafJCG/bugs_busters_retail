# Libraries -----------------------------------------------------------------------------------
import os
import sys

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import pickle
# Append current working directory
sys.path.append(os.getcwd())

# Function -----------------------------------------------------------------------------------
def ml_kmean(path):
    # abrir archivo ya procesado
    df_processed = pd.read_parquet("files/datasets/intermediate/df_processed.parquet")
    # seleccionar ventas
    df_ventas = df_processed[df_processed['transaction_type'] == 'ventas']

    # calcular rfm
    latest_date_v = df_ventas['invoice_date'].max()
    recency_df_v = df_ventas.groupby('customer_id').agg({'invoice_date': lambda x: (latest_date_v - x.max()).days})
    recency_df_v.rename(columns={'invoice_date': 'recency'}, inplace=True)
    recency_df_v = remove_outliers(recency_df_v, ['recency'])

    frequency_df_v = df_ventas.groupby('customer_id').agg({'invoice_no': 'count'})
    frequency_df_v.rename(columns={'invoice_no': 'frequency'}, inplace=True)
    frequency_df_v = remove_outliers(frequency_df_v, ['frequency'])

    monetary_df_v = df_ventas.groupby('customer_id').agg({'total_cost': 'sum'})
    monetary_df_v.rename(columns={'total_cost': 'monetary'}, inplace=True)
    monetary_df_v = remove_outliers(monetary_df_v, ['monetary'])

    # calcular rfm
    latest_date_v = df_ventas['invoice_date'].max()
    recency_df_v = df_ventas.groupby('customer_id').agg({'invoice_date': lambda x: (latest_date_v - x.max()).days})
    recency_df_v.rename(columns={'invoice_date': 'recency'}, inplace=True)
    recency_df_v = remove_outliers(recency_df_v, ['recency'])

    frequency_df_v = df_ventas.groupby('customer_id').agg({'invoice_no': 'count'})
    frequency_df_v.rename(columns={'invoice_no': 'frequency'}, inplace=True)
    frequency_df_v = remove_outliers(frequency_df_v, ['frequency'])

    monetary_df_v = df_ventas.groupby('customer_id').agg({'total_cost': 'sum'})
    monetary_df_v.rename(columns={'total_cost': 'monetary'}, inplace=True)
    monetary_df_v = remove_outliers(monetary_df_v, ['monetary'])

    #unir rfm en un archivo

    rfm_df_v = recency_df_v.join(frequency_df_v).join(monetary_df_v)
    rfm_df_v.dropna(inplace=True)

    #Método de codo

    # Extraer las variables RFM en un nuevo DataFrame
    

    X = rfm_df_v[['recency', 'frequency', 'monetary']].values
    wcss = []
    for i in range(1, 11):
        kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=0)
        kmeans.fit(X)
        wcss.append(kmeans.inertia_)
        # Paso 2: Crear la línea desde el primer al último punto
    p1 = np.array([1, wcss[0]])
    p2 = np.array([10, wcss[-1]])

    # Paso 3: Calcular las distancias de cada punto a esta línea
    distances = []
    for i in range(len(wcss)):
        p = np.array([i+1, wcss[i]])
        distance = np.abs(np.cross(p2-p1, p1-p))/np.linalg.norm(p2-p1)
        distances.append(distance)

    # Paso 4: El punto con la distancia máxima es el "codo"
    n_clusters_optimo = distances.index(max(distances)) + 1

    print(f"El número óptimo de clusters es: {n_clusters_optimo}")

    # Guardar info cluster en el archivo rfm


    rfm_df_v['cluster'] = kmeans.fit_predict(X) #guardar cluster

    #renombrar clusters
    rfm_df_v['cluster'] = rfm_df_v['cluster'].replace(0, 'oro')
    rfm_df_v['cluster'] = rfm_df_v['cluster'].replace(1, 'bronce')
    rfm_df_v['cluster'] = rfm_df_v['cluster'].replace(2, 'plata')

    #archivo final
    df_final_1 = pd.merge(df_processed, rfm_df_v, on='customer_id', how='inner')
    df_final = df_final_1[df_final_1['cluster'].isin(['oro', 'bronce', 'plata'])]
    # Guardar Archivo final
    df_final.to_parquet('files/datasets/output/df_final.parquet', index=False)
    
    # Guarda el modelo
    kmeans = KMeans(n_clusters=n_clusters_optimo).fit(X)
    pickle.dump(kmeans, open('files/modeling_output/model_fit/kmeans_retail.pickle', 'wb'))


    # fig, ax = plt.subplots(1, 2, figsize=(10, 5))

    # # Subplot 1: Método del Codo
    # ax[0].plot(range(1, 11), wcss, marker='o')
    # ax[0].set_title('Método del Codo')
    # ax[0].set_xlabel('Número de clusters')
    # ax[0].set_ylabel('Inercia')

    # # Subplot 2: Gráfica de los Clusters
    # ax[1].remove()
    # ax[1] = fig.add_subplot(1, 2, 2, projection='3d')

    # # Graficar los clusters
    # kmeans = KMeans(n_clusters=n_clusters_optimo).fit(X)
    # centroids = kmeans.cluster_centers_
    # labels = kmeans.predict(X)
    # C = kmeans.cluster_centers_
    # colores = ['red', 'green', 'blue']
    # asignar = [colores[row] for row in labels]

    # ax[1].scatter(X[:, 0], X[:, 1], X[:, 2], c=asignar, s=60)
    # ax[1].scatter(C[:, 0], C[:, 1], C[:, 2], marker='*', c=colores, s=1000)
    # ax[1].set_title('Visualización de Clusters')
    # ax[1].set_xlabel('Recency')
    # ax[1].set_ylabel('Frequency')
    # ax[1].set_zlabel('Monetary')

    # plt.tight_layout()
    # plt.show()

def remove_outliers(df, column1 = 'quantityt', column2 = 'unit_price'):
    """
    Elimina outliers de una columna específica del DataFrame.

    Parameters:
        df: El DataFrame que se va a modificar.
        column (str): El nombre de la columna de la que se eliminarán los outliers.

    Returns:
        pd.DataFrame: El DataFrame sin outliers en la columna especificada.
    """
    Q1 = df[column1].quantile(0.25)
    Q3 = df[column1].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df = df[(df[column1] >= lower_bound) & (df[column1] <= upper_bound)]
    return df
    Q1 = df[column2].quantile(0.25)
    Q3 = df[column2].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df = df[(df[column2] >= lower_bound) & (df[column2] <= upper_bound)]
    return df

# Execution -----------------------------------------------------------------------------------
ml_kmean("files/datasets/intermediate/df_processed.parquet")