# Libraries 
import pandas as pd
import os 

def process_dataset(file_path, output_directory, file_type='csv', **kwargs):
    """
    Procesa el dataset desde la carga hasta la limpieza y guarda el resultado final, eliminando archivos intermedios.

    Parameters:
        file_path (str): La ruta del archivo de entrada.
        output_directory (str): El directorio donde se guardarán los archivos de salida.
        file_type (str): El tipo de archivo para guardar ('csv' o 'excel'). El valor por defecto es 'csv'.
        **kwargs: Argumentos adicionales que se pasarán a la función de carga y guardado.

    Returns:
        pd.DataFrame: El DataFrame procesado.
    """
    df = load_dataset(file_path, file_type, **kwargs)
    if df is None:
        return None

    intermediate_paths = []

    # Paso 1: Limpieza de datos
    df, _ = clear_data(df)
    intermediate_path = f"{output_directory}/df_step1.{file_type}"
    save_dataset(df, intermediate_path, file_type, index=False)
    intermediate_paths.append(intermediate_path)

    # Paso 2: Eliminar outliers
    df = clean_data(df)
    intermediate_path = f"{output_directory}/df_step2.{file_type}"
    save_dataset(df, intermediate_path, file_type, index=False)
    intermediate_paths.append(intermediate_path)

    # Paso 3: Categorizar transacciones
    df = categorize_transaction(df)
    intermediate_path = f"{output_directory}/df_step3.{file_type}"
    save_dataset(df, intermediate_path, file_type, index=False)
    intermediate_paths.append(intermediate_path)

    # Guardar el dataset final
    final_path = f"{output_directory}/df_processed.{file_type}"
    save_dataset(df, final_path, file_type, index=False)

    # Eliminar archivos intermedios
    remove_intermediate_files(intermediate_paths)

    return df