# Libraries -----------------------------------------------------------------------------------
import pandas as pd

# Paths -----------------------------------------------------------------------------------
file_path = 'files/datasets/input/Online_Retail.csv'
output_directory =  'files/datasets/intermediate/'

# Functions -----------------------------------------------------------------------------------
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

    # Paso 1: Limpieza de datos
    df,_ = clear_data(df)
    intermediate_path = f"{output_directory}df_step1.{file_type}"
    save_dataset(df, intermediate_path, file_type, index=False)

    # Paso 2: Eliminar outliers
    df = clean_data(df)
    intermediate_path = f"{output_directory}df_step2.{file_type}"
    save_dataset(df, intermediate_path, file_type, index=False)

    # Paso 3: Categorizar transacciones
    df = categorize_transaction(df)
    intermediate_path = f"{output_directory}df_step3.{file_type}"
    save_dataset(df, intermediate_path, file_type, index=False)

    # Guardar el dataset final
    final_path = f"{output_directory}df_processed.{file_type}"
    save_dataset(df, final_path, file_type, index=False)

def load_dataset(file_path, file_type='csv', sep=',', encoding='latin1'):
    """
    Carga un dataset desde una ruta de archivo y crea las columnas necesarias para continuar con el análisis.

    Parameters:
        file_path (str): La ruta del archivo.
        file_type (str): Se puede especificar el tipo de archivo ('csv' o 'excel'). El valor por defecto es 'csv'.
        **kwargs: Argumentos adicionales que se pasarán a la función de carga: headers, sep, encoding, etc.

    Returns:
        df: El dataset cargado y con nombres de columnas ajustados desde el archivo.
    """
    try:
        if file_type == 'csv':
            df = pd.read_csv(file_path, sep=sep, encoding=encoding)
        elif file_type == 'excel':
            df = pd.read_excel(file_path, sep=sep, encoding=encoding)
        else:
            print(f"Tipo de archivo {file_type} no soportado.")
            return None
        print(f"Dataset cargado desde {file_path}.")
        df = encabezados(df)
        df['total_cost'] = df['quantity'] * df['unit_price']
        df['invoice_date'] = pd.to_datetime(df['invoice_date'], format="%d/%m/%Y %H:%M")
        df['month'] = df['invoice_date'].dt.month
        df['year'] = df['invoice_date'].dt.year
        print("Encabezados ajustados.")
        print(df.head(10))
        print(df.info())
        return df
    except FileNotFoundError:
        print(f"El archivo {file_path} no se encontró.")
    except pd.errors.EmptyDataError:
        print("El archivo está vacío.")
    except pd.errors.ParserError:
        print("Error al analizar el archivo.")
    except Exception as e:
        print(f"Ocurrió un error: {e}")
        return None

def encabezados(df):
    '''
    Renombra las columnas para que estén de acuerdo al estándar 'snake_case'.
    '''
    new_col_names = []
    for name in df.columns:
        name_lowered = name.lower()
        name_stripped = name_lowered.strip()
        name_no_spaces = name_stripped.replace(' ', '_')
        name_string = str(name_no_spaces)
        new_col_names.append(name_string)
    df.columns = new_col_names
    return df

def clear_data(df, columns=['description', 'customer_id'], critic_columns=['quantity', 'unit_price', 'customer_id', 'total_cost']):
    """
    Esta función elimina duplicados explícitos.
    Se pueden seleccionar columnas para rellenar con 'unknown' valores nulos.

    Parameters:
        df : dataset a limpiar.
        critic_columns (list): Lista de columnas que se analizarán para eliminar valores nulos.
        columns (list): Lista de columnas que se analizarán para cambiar valores nulos por 'unknown'.

    Returns:
        df: dataset limpio.
        dict: El número de valores nulos encontrados antes de la limpieza.
    """
    if df is None:
        print("El DataFrame está vacío. No se puede proceder con la limpieza de datos.")
        return None, {}
    df = df.drop_duplicates()
    df = df.dropna(subset=critic_columns)
    null_counts = {}
    for column in columns:
        null_count = df[column].isnull().sum()
        null_counts[column] = null_count
        print(f"Valores nulos en la columna '{column}': {null_count}")
        if null_count > 0:
            df[column] = df[column].fillna('unknown')
    return df, null_counts

def save_dataset(df, output_path, file_type='csv', **kwargs):
    """
    Guarda un dataset en un archivo.

    Parameters:
        df: El DataFrame que se va a guardar.
        output_path (str): La ruta del archivo de salida.
        file_type (str): El tipo de archivo para guardar ('csv' o 'excel'). El valor por defecto es 'csv'.
    """
    try:
        if file_type == 'csv':
            df.to_csv(output_path, **kwargs)
        elif file_type == 'excel':
            df.to_excel(output_path, **kwargs)
        else:
            print(f"Tipo de archivo {file_type} no soportado.")
        print(f"Dataset guardado en {output_path}.")
    except Exception as e:
        print(f"Ocurrió un error al guardar el archivo: {e}")

def categorize_transaction(df, column1='stock_code', column2='quantity'):
    """
    Función que categoriza dependiendo de la columna seleccionada para evaluar el tipo de operación: otros, ventas, devoluciones.

    Parameters:
        df: El dataset que se va a modificar.
        column1 (str): El nombre de la columna que se va a evaluar, tiene como valor por defecto 'stock_code'.
        column2 (str): El nombre de la columna que se va a evaluar, tiene como valor por defecto 'quantity'.

    Returns:
        pd.DataFrame: El DataFrame con la nueva columna 'transaction_type' que indica la categoría de la operación.
    """
    if df is None:
        print("El DataFrame está vacío. No se puede proceder con la categorización de transacciones.")
        return None

    categorias_especificas = ['amazonfee', 'post', 's', 'pads', 'cruk', 'dcgssgirl', 'm', 'd', 'dot']

    def categorize(row):
        stock_code = row[column1]
        quantity = row[column2]
        if stock_code in categorias_especificas:
            return 'otros'
        elif quantity > 0:
            return 'ventas'
        elif quantity < 0:
            return 'devoluciones'
        else:
            return 'otros'

    df['transaction_type'] = df.apply(categorize, axis=1)
    print(df.head())
    return df

def clean_data(df, column1= 'quantity', column2= 'unit_price'):
    """
    Limpia el dataset eliminando duplicados, filas con valores nulos en las columnas críticas y outliers.

    Parameters:
        df: El datasert que se va a limpiar.

    Returns:
        df: El dataset limpio.
    """

    # Eliminar filas con unit_price igual a 0.0
    df = df[df['unit_price'] != 0.0]

    # Eliminar duplicados
    df = df.drop_duplicates()

    # Eliminar filas con valores nulos en las columnas críticas
    df = df.dropna(subset=['quantity', 'unit_price', 'customer_id', 'total_cost'])

    # Eliminar outliers
    df = remove_outliers(df, column1)
    df = remove_outliers(df, column2)

    return df

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

# Run Functions -----------------------------------------------------------------------------------
process_dataset(file_path, output_directory)
