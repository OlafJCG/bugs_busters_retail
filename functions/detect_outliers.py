# Libraries -----------------------------------------------------------------------------------
import pandas as pd

def IQR_upper_lower_levels(series):
    """
    Calculates the upper and lower levels based on the Interquartile Range (IQR) for a given data series.

    This function computes the first quartile (Q1) and the third quartile (Q3) to determine the Interquartile Range (IQR).
    It then calculates the lower and upper bounds for identifying outliers using the formula:
    - Lower level = Q1 - 1.5 * IQR
    - Upper level = Q3 + 1.5 * IQR

    Args:
        series (pandas.Series): A numerical series for which the bounds will be calculated.

    Returns:
        tuple: A tuple containing the lower and upper levels.
               - lower_level (float): The lower bound for detecting outliers.
               - upper_level (float): The upper bound for detecting outliers.

    Example:
        >>> import pandas as pd
        >>> data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        >>> IQR_upper_lower_levels(data)
        (-2.0, 12.0)
    """
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_level = Q1 - 1.5 * IQR
    upper_level = Q3 + 1.5 * IQR
    return lower_level, upper_level