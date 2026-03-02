from constant.constants.pandas_constant import PandasConstant
import pandas as pd

class PandasUtil:


    @classmethod
    def cast_object_columns_to_float64(cls,df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert all object-typed columns to float64.
        """
        if df.empty:
            return df

        return df.astype({
            col: PandasConstant.FLOAT64_DTYPE.value
            for col in df.columns
            if df[col].dtype == PandasConstant.OBJECT_DTYPE.value
        })