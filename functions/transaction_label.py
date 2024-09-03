# Libraries -----------------------------------------
import pandas as pd

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
