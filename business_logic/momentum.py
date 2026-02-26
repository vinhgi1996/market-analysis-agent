from constant.momentum_sql_queries import MomentumSQLQueries
from util.postgre_sql import PostgresSQLUtil


def momentum_metrics_synthesys(date:str):
    print("Momentum metrics synthesys started.")
    data =  PostgresSQLUtil.run_sql(MomentumSQLQueries.GET_DATA_BY_DATE,(date,))
    print(data)

if __name__ == "__main__":
    momentum_metrics_synthesys('2026-02-25')